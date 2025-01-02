const express = require('express');
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const winston = require('winston');
const { error } = require('console');

class LocalCenterManager {
    constructor() {
        this.app = express();
        this.app.use(express.json());

        this.configg = this.loadDatacenterConfig('datacenter_config.json');
        this.config = this.configg["LMs"];
        this.LM_config = { partitions: {} };
        // this.LM_id = 1;

        const podName = process.env.LM_ID; // e.g., "lm-0"
        if (!podName) {
            throw new Error("LM_ID environment variable is not set. Ensure Kubernetes configuration is correct.");
        }

        // Parse the ordinal index from the pod name to set LM_ID
        const lmIdMatch = podName.match(/lm-(\d+)/);
        if (lmIdMatch) {
            this.LM_id = parseInt(lmIdMatch[1], 10) + 1; // Convert "lm-0" to LM ID 1, "lm-1" to LM ID 2, etc.
        } else {
            throw new Error(`Invalid pod name format: ${podName}`);
        }

        console.log(`Logical Master (LM) ID set to: ${this.LM_id}`);

        this.GM_id = null;
        this.GM_config = this.configg["GMs"];

        // Initialize inconsistency logger
        this.inconsistencyLogger = this.setupInconsistencyLogger();

        // Initialize LM configuration
        for (let [partitionId, partitionData] of Object.entries(this.config[this.LM_id]["partitions"])) {
            const gmId = partitionData["gm_id"];
            const workerNodes = partitionData["worker_nodes"];
            const additionNodes = {};

            workerNodes.forEach(node => {
                additionNodes[node] = true;
            });

            this.LM_config.partitions[partitionId] = {
                "gm_id": gmId,
                "worker_nodes": additionNodes
            };
        }
        this.LM_config[this.LM_id] = {
            ...this.LM_config[this.LM_id],
            address: this.config[this.LM_id].address
        }

        // console.log(this.LM_config.partitions);

        // Inconsistency counter
        this.inconsistencyCount = 0;

        this.receiveInformation();
    }

    loadDatacenterConfig(filename) {
        const configPath = path.join(__dirname, filename);
        return JSON.parse(fs.readFileSync(configPath, 'utf8'));
    }

    setupInconsistencyLogger(logFile = 'inconsistencies.log') {
        const logDir = path.dirname(logFile);
        if (!fs.existsSync(logDir)) {
            fs.mkdirSync(logDir, { recursive: true });
        }

        return winston.createLogger({
            level: 'info',
            format: winston.format.combine(
                winston.format.timestamp(),
                winston.format.printf(({ timestamp, level, message }) => {
                    return `${timestamp} - ${level}: ${message}`;
                })
            ),
            transports: [
                new winston.transports.File({ filename: logFile })
            ]
        });
    }

    getStatus() {
        const statusUpdate = {};
        for (let [partitionId, gmPartition] of Object.entries(this.LM_config.partitions)) {
            const workerNodes = gmPartition["worker_nodes"];
            Object.entries(workerNodes).forEach(([nodeId, availability]) => {
                if (availability) {
                    statusUpdate[partitionId] = nodeId;
                }
            });
        }
        return JSON.parse(JSON.stringify(statusUpdate)); // Deep copy
    }

    async sendStatusUpdate(gmId) {
        // console.log("GM Config:", this.GM_config);
        // console.log("GM ID:", gmId);

        if (!this.GM_config || !this.GM_config[gmId]) {
            console.error(`Invalid GM ID: ${gmId} or GM Config not initialized.`);
            return;
        }

        const gmAddress = this.GM_config[gmId]["address"];
        const statusSend = this.getStatus();
        try {
            await axios.post(`http://${gmAddress}/update_gm`, { status: statusSend });
        } catch (error) {
            console.error(`Failed to update worker node availability to GM ${gmId}`);
        }
    }


    async sendStatusUpdateToAllGm(currentTime) {
        const statusSend = this.getStatus();
        for (let [gmId, gmData] of Object.entries(this.GM_config)) {
            const gmAddress = gmData["address"];
            try {
                await axios.post(`http://${gmAddress}/update_gm`, { status: statusSend, lmId: this.this.LM_id });
            } catch (error) {
                console.error(`Failed to update worker node availability to GM ${gmId}`);
            }
        }
    }

    receiveInformation() {
        // Process Tasks Route
        this.app.post('/process_tasks', async (req, res) => {
            const { task_batch } = req.body;
            // console.log(task_batch);
            const response = await this.verifyRequests(task_batch);
            res.json(response);
        });

        this.app.post("/assign_tasks_waiting", async (req, res) => {
            // console.log("task for waiting queue received");
            const { task } = req.body;
            const nodeId = task.nodeId;
            const reponse = await this.launchTaskOnWorkerNode(task, nodeId);
            res.json(reponse);

        });

        // Worker Node Response Route
        this.app.post('/workernode_response', async (req, res) => {
            // console.log("Response from worker node");
            const { task_response } = req.body;
            const response = await this.taskCompleted(task_response);
            res.status(200).json(response);
        });

        // LM Request Update Route
        this.app.post('/lmrequestupdate', async (req, res) => {
            await this.sendStatusUpdateToAllGm();
            res.sendStatus(200);
        });
    }

    async verifyRequests(taskMappings) {
        for (let taskMapping of taskMappings) {
            const task = taskMapping.task;
            const nodeId = task.nodeId;
            // console.log(nodeId, "nodeID type:", typeof (nodeId));
            if (taskMapping.externalpartition) {
                const externalPartitionId = taskMapping.externalpartition;
                let gm_partition = this.LM_config.partitions[externalPartitionId];
                // console.log("Partition details:", gm_partition);
                if (gm_partition && gm_partition.worker_nodes[nodeId]) {
                    gm_partition.worker_nodes[nodeId] = false;
                    task.partitionId = externalPartitionId;

                    // console.log("sending to worker function");
                    await this.launchTaskOnWorkerNode(task);
                } else {
                    task.inconsistencies++;
                    this.inconsistencyCount++;
                    // console.log("Inconsistent task mapping:", task);
                    await this.handleInconsistency(task, new Date());
                }
            } else {
                let gm_partition = this.LM_config.partitions[task.gmId];
                // console.log("Partition details:", gm_partition);
                if (gm_partition && gm_partition.worker_nodes[nodeId]) {
                    gm_partition.worker_nodes[nodeId] = false;
                    task.partitionId = task.gmId;
                    // console.log("sending to worker function");
                    await this.launchTaskOnWorkerNode(task, nodeId);
                } else {
                    task.inconsistencies++;
                    // console.log("Inconsistent task mapping:", task);
                    this.inconsistencyCount++;
                    await this.handleInconsistency(task, new Date());
                }
            }
        }


    }

    async handleInconsistency(task, currentTime) {

        // console.log("inconsistant task:", task);

        this.inconsistencyLogger.info(
            `Inconsistency detected: GM_ID=${task.gmId}, Partition_ID=${task.partitionId}, ` +
            `Task_ID=${task.task_id}, Time=${currentTime}, ` +
            `Total_Inconsistencies=${this.inconsistencyCount}`
        );


        const gmAddress = task.gmNodeAddress;
        const gm_ID = task.gmId;
        // console.log("gm id of tasks:", gm_ID);

        await this.sendStatusUpdate(gm_ID);

        try {
            await axios.post(`http://${gmAddress}/unschedule_the_task`, { task: task });
        } catch (error) {
            this.inconsistencyLogger.error(
                `Failed to update GM ${task.gmId} after inconsistency: Task_ID=${task.task_id}`
            );
        }



        this.inconsistencyLogger.info(`Total number of inconsistencies so far: ${this.inconsistencyCount}`);
    }

    async launchTaskOnWorkerNode(task, nodeId) {
        // console.log("sending worker node");
        try {
            await axios.post(`http://${nodeId}/worker_node`, {
                task: task,
                lm_address: this.LM_config[this.LM_id]["address"]
            });
        } catch (error) {
            console.error(`Failed to send task to workernode with ip ${nodeId}`, error);
        }
        // console.log("sent to wrkernode", nodeId);
    }

    async taskCompleted(task) {
        // console.log(task);

        try {
            // Mark the worker node as available again
            const partitionId = task.partitionId;
            const nodeId = task.nodeId;

            if (
                this.LM_config.partitions[partitionId] &&
                this.LM_config.partitions[partitionId].worker_nodes[nodeId] !== undefined
            ) {
                this.LM_config.partitions[partitionId].worker_nodes[nodeId] = true;
                console.log(`Worker node ${nodeId} in partition ${partitionId} is now available.`);
            } else {
                console.warn(`Worker node ${nodeId} or partition ${partitionId} not found in LM configuration.`, error);
            }

            // Send the task completion status to the Global Master (GM)
            const gmId = task.gmId;
            const gmAddress = task.gmNodeAddress;
            if (!gmAddress) {
                console.error(`GM address not found for GM ID ${gmId}`, error);
                return;
            }

            const response = await axios.post(`http://${gmAddress}/task_complete`, { task: task });
            console.log(`Task completion status sent to GM ${gmId}.`);
            return { resopnse: "received by gm" };

        } catch (error) {
            console.error(`Failed to update task completion to GM ${task.gmId}. Error:`, error);
        }
        return { resopnse: " not received by gm" };
    }


    run() {
        const PORT = 5000;
        this.app.listen(PORT, '0.0.0.0', () => {
            console.log(`Local Center Manager running on port ${PORT}`);
        });
    }
}

// Example usage
const manager = new LocalCenterManager();
manager.run();

module.exports = LocalCenterManager;