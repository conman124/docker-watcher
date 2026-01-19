const http = require("node:http");
const EventEmitter = require("node:events");

function makeRequest(path, cb) {
    const host = process.env["DOCKER_HOST"] || "unix:///var/run/docker.sock";
    const opts = {path};
    if(host.startsWith("unix://")) {
        const sock = host.replace("unix://", "");
        opts.socketPath = sock;
    } else if (host.startsWith("tcp://")) {
        const hostAndPort = host.replace("tcp://", "");
        let [hostname,port] = hostAndPort.split(":");
        if(!port) { port = 2375; }
        opts.hostname = hostname;
        opts.port = port;
    } else {
        throw new Error("I don't know how to handle that DOCKER_HOST: " + host);
    }

    return http.request(opts, cb);
}

function isResponseSuccessful(res) {
    return res.statusCode >= 200 && res.statusCode < 299;
}

function retryOrRejectRequest(err, retry, i, resolve, reject) {
    const waits = [0, 50, 500, 5000]
    if(i >= 4) {
        reject(err);
    } else {
        setTimeout(() => {
            retry(i+1, resolve, reject);
        }, waits[i]);
    }
}

class DockerPinger extends EventEmitter {
    #stopped = false;

    constructor() {
        super();

        this.#ping();
    }

    #ping() {
        if(this.#stopped) { return; }

        const req = makeRequest(`/v1.39/_ping`, res => {
            let successful = isResponseSuccessful(res);

            if(!successful) {
                this.emit("closed");
            }

            res.on("data", () => {}); // We don't care about the data, but we have to eat it or end never gets called

            res.on("end", () => {
                if(successful) {
                    setTimeout(() => {this.#ping()}, 20000);
                }
            });
        });

        req.on("error", () => this.emit("closed"));
        req.end();
    }

    stop() {
        this.#stopped = true;
    }
}

class DockerWatcher extends EventEmitter {
    #label;
    #currentContainers;
    #lastKnownEventTimestamp;

    constructor(label) {
        super();
        this.#label = label;
        this.#currentContainers = {};
        this.#lastKnownEventTimestamp = 0;

        this.on("newListener", (event, callback) => {
            if(event === "containersChanged") {
                if(Object.keys(this.#currentContainers).length) {
                    process.nextTick(() => {
                        callback(this.#cloneContainers());
                    });
                }
            }
        });

        this.#initializeContainers().catch(err => {
            this.emit("error", err);
        });
    }

    #getCurrentContainers() {
        const filters = {}
    
        if(this.#label) {
            filters.label = [this.#label];
        }
    
        const filterStr = encodeURIComponent(JSON.stringify(filters));

        function tryRequest(i, resolve, reject) {
            console.log("docker-watcher: Getting current containers")
            const req = makeRequest(`/v1.39/containers/json?filters=${filterStr}`, res => {
                console.log("docker-watcher: Getting current containers - request")
                let data = "";
                let successful = isResponseSuccessful(res);
    
                res.on("data", (chunk) => {
                    console.log("docker-watcher: Getting current containers - data")
                    data += chunk;
                });
                res.on("end", () => {
                    console.log("docker-watcher: Getting current containers - end")
                    if(successful) {
                        try {
                            resolve(JSON.parse(data));
                        } catch (e) {
                            retryOrRejectRequest("Failed to parse data: " + e, tryRequest, i, resolve, reject);
                        }
                    } else {
                        retryOrRejectRequest(`Got ${res.statusCode}: ${data}`, tryRequest, i, resolve, reject);
                    }
                });
            });
    
            req.on("error", (e) => {console.log("docker-watcher: Getting current containers - error"); retryOrRejectRequest(e, tryRequest, i, resolve, reject)});
            req.end();
        }
    
        return new Promise((resolve, reject) => {
            tryRequest(0, resolve, reject);
        });
    }

    #watchContainers() {
        const filters = {
            type: ["container"],
            event: ["start", "die"],
        }
    
        if(this.#label) {
            filters.label = [this.#label];
        }
    
        const filterStr = encodeURIComponent(JSON.stringify(filters));

        const tryRequest = (i, neverResolve, reject) => {
            console.log("docker-watcher: Watching containers")
            // Technically there are two race conditions here, both seem equally unlikely and
            // difficult to resolve:
            //
            // 1. If there are no containers when we initially fetch the list and then they are created
            //    before we start streaming events. (lastKnownEventTimestamp will be 0)
            // 2. If the streaming request dies, then an event is emitted in the same ms as the last
            //    event we saw. (since includes the ms passed, so to not get some duplicates, we bump
            //    it by 1)
            const since = this.#lastKnownEventTimestamp ? "&since="+(this.#lastKnownEventTimestamp+1) : "";

            // This is used to close the event stream if the server stops responding to pings.
            let eventStreamResponse;
            const pinger = new DockerPinger();
            pinger.on("closed", () => {
                console.log("docker-watcher: Watching containers - pinger closed")
                if(eventStreamResponse) {
                    // There is a current event streaming response, but Docker has stopped or crashed (is
                    // not responding to ping API), so manually end the event stream response and let the
                    // retry mechanism kick in
                    eventStreamResponse.emit("end");
                }
            });

            const req = makeRequest(`/v1.39/events?filters=${filterStr}${since}`, res => {
                console.log("docker-watcher: Watching containers - request")
                eventStreamResponse = res;

                let data = "";
                let successful = isResponseSuccessful(res);

                res.on("data", (chunk) => {
                    console.log("docker-watcher: Watching containers - data")
                    data += chunk;
                    if(successful) {
                        data = this.#handleEvents(data);
                    }
                });
                res.on("end", () => {
                    console.log("docker-watcher: Watching containers - end")
                    if(successful) {
                        // Not sure why this got closed, but since it was not an error per se, retry starting at retry 0
                        retryOrRejectRequest("", tryRequest, 0, neverResolve, reject);
                    } else {
                        retryOrRejectRequest(`Got ${res.statusCode}: ${data}`, tryRequest, i, neverResolve, reject);
                    }
                });
            });

            req.on("error", (e) => {console.log("docker-watcher: Watching containers - error"); retryOrRejectRequest(e, tryRequest, i, neverResolve, reject)});
            req.end();
        }

        new Promise((neverResolve, reject) => {
            tryRequest(0, neverResolve, reject);
        }).catch(e => this.emit("error", e));
    }

    #handleEvents(data) {
        let index;
        const newContainers = [];
        const removeIds = [];
        while((index = data.indexOf("\n")) != -1) {
            const event = data.slice(0, index+1);
            data = data.slice(index+1);

            const parsed = JSON.parse(event);

            if(parsed.status === "start") {
                newContainers.push({id: parsed.id, labels: parsed.Actor.Attributes});
            } else if (parsed.status === "die") {
                removeIds.push(parsed.id);
            } else {
                console.error("Got unexpected event, ignoring:", event);
            }
            this.#lastKnownEventTimestamp = parsed.time;
        }
        this.#updateContainers(newContainers, removeIds);
        return data;
    }

    #updateContainers(newContainers, removeContainerIds) {
        newContainers.forEach(container => {
            this.#currentContainers[container.id] = container;
        });

        removeContainerIds.forEach(id => {
            delete this.#currentContainers[id];
        });

        this.#emitChanged();
    }

    #insertContainers(containers) {
        this.#updateContainers(containers, []);
    }

    #debounceTimer = undefined;

    #emitChanged() {
        if(this.#debounceTimer) {
            clearTimeout(this.#debounceTimer);
        }
        this.#debounceTimer = setTimeout(() => {
            this.emit("containersChanged", this.#cloneContainers());
        }, 250);
    }

    #cloneContainers() {
        return structuredClone(this.#currentContainers);
    }

    async #initializeContainers() {
        const currentContainers = await this.#getCurrentContainers();
        if(currentContainers.length) {
            this.#lastKnownEventTimestamp = Math.max(...currentContainers.map(c => c.Created));
        } else {
            this.#lastKnownEventTimestamp = 0;
        }
        this.#insertContainers(currentContainers.map(c => ({
            id: c.Id,
            labels: c.Labels,
        })));
        this.#watchContainers();
    }
}

module.exports = {
    DockerWatcher,
}
