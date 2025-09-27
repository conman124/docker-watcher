const http = require("node:http");
const EventEmitter = require("node:events");

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

        const req = http.request({
            socketPath: "/var/run/docker.sock",
            path: `/v1.39/_ping`,
        }, res => {
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
            const req = http.request({
                socketPath: "/var/run/docker.sock",
                path: `/v1.39/containers/json?filters=${filterStr}`,
            }, res => {
                let data = "";
                let successful = isResponseSuccessful(res);
    
                res.on("data", (chunk) => {
                    data += chunk;
                });
                res.on("end", () => {
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
    
            req.on("error", (e) => {retryOrRejectRequest(e, tryRequest, i, resolve, reject)});
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
            // Technically there are two race conditions here, both seem equally unlikely and
            // difficult to resolve:
            //
            // 1. If there are no containers when we initially fetch the list and then they are created
            //    before we start streaming events. (lastKnownEventTimestamp will be 0)
            // 2. If the streaming request dies, then an event is emitted in the same ms as the last
            //    event we saw. (since includes the ms passed, so to not get some duplictates, we bump
            //    it by 1)
            //
            // Another unexpected issue is if docker goes down with containers running, when you restart
            // those containers, their "Created" timestamp seems to still be the old one, instead of the
            // new one.  So you can get some double events when starting up.  I am not terribly worried
            // about this, but one solution could be to not emit our first containersChanged event until
            // we've fetched the initial list, and streamed all the "backlogged" events.
            const since = this.#lastKnownEventTimestamp ? "&since="+(this.#lastKnownEventTimestamp+1) : "";

            // Used to close the event stream if the server stops responding to pings.
            let eventStreamResponse;
            const pinger = new DockerPinger();
            pinger.on("closed", () => {
                if(eventStreamResponse) {
                    // There is a current event streaming response, but Docker has stopped or crashed (is
                    // not responding to ping API), so manually end the event stream response and let the
                    // retry mechanism kick in
                    eventStreamResponse.emit("end");
                }
            });

            const req = http.request({
                socketPath: "/var/run/docker.sock",
                path: `/v1.39/events?filters=${filterStr}${since}`,
            }, res => {
                eventStreamResponse = res;

                let data = "";
                let successful = isResponseSuccessful(res);

                res.on("data", (chunk) => {
                    data += chunk;
                    if(successful) {
                        data = this.#handleEvents(data);
                    }
                });
                res.on("end", () => {
                    if(successful) {
                        // Not sure why this got closed, but since it was not an error per se, retry starting at retry 0
                        retryOrRejectRequest("", tryRequest, 0, neverResolve, reject);
                    } else {
                        retryOrRejectRequest(`Got ${res.statusCode}: ${data}`, tryRequest, i, neverResolve, reject);
                    }
                });
            });

            req.on("error", (e) => retryOrRejectRequest(e, tryRequest, i, neverResolve, reject));
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
                newContainers.push(parsed.Actor);
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
            this.#currentContainers["ID" in container ? container.ID : container.Id] = container;
        });

        removeContainerIds.forEach(id => {
            delete this.#currentContainers[id];
        });

        this.#emitChanged();
    }

    #insertContainers(containers) {
        this.#updateContainers(containers, []);
    }

    #emitChanged() {
        this.emit("containersChanged", this.#cloneContainers());
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
        this.#insertContainers(currentContainers);
        this.#watchContainers();
    }
}

module.exports = {
    DockerWatcher,
}