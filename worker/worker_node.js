const express = require('express');
const axios = require('axios');

const app = express();
app.use(express.json());

// Worker Node Class
class WorkerNode {
    constructor(lmAddress) {
        this.lmAddress = lmAddress;
    }

    // Simulate Workload Execution
    executeWorkload(duration) {
        const startTime = Date.now();
        while ((Date.now() - startTime) / 1000 < duration) {
            // Simulate computation
            Math.sqrt(12345) * Math.sin(12345) * Math.tan(12345);
        }
        return Date.now();
    }

    // Process Task and Notify Local Master
    async processTask(task) {
        task.scheduledTime = Date.now(); // Record task start time
        task.endTime = this.executeWorkload(task.duration); // Simulate task execution

        try {
            const response = await this.notifyLocalMasterWithRetry(task);
            console.log('Task successfully reported to Local Master:', response.status);
            return response;
        } catch (error) {
            console.error('Failed to notify Local Master after retries:', error.message);
            return null;
        }
    }

    // Notify Local Master of Task Completion with retry logic
    async notifyLocalMasterWithRetry(task, retries = 3, delay = 1000) {
        let attempt = 0;
        while (attempt < retries) {
            try {
                const response = await this.notifyLocalMaster(task);
                if (response.status === 200) {
                    return response; // If successful, return the response
                } else {
                    throw new Error(`Non-200 status: ${response.status}`);
                }
            } catch (error) {
                attempt++;
                console.error(`Attempt ${attempt} failed: ${error.message}`);
                if (attempt < retries) {
                    console.log(`Retrying in ${delay}ms...`);
                    await this.delay(delay); // Wait before retrying
                    delay *= 2; // Exponential backoff
                } else {
                    throw new Error('All retry attempts failed');
                }
            }
        }
    }

    // Notify Local Master (existing function)
    async notifyLocalMaster(task) {
        const url = `http://${this.lmAddress}/workernode_response`;
        console.log(`Sending task completion to Local Master at ${url}`);
        return await axios.post(url, { task_response: task });
    }

    // Utility function to introduce delay
    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

// API Endpoint for Worker Node
app.post('/worker_node', async (req, res) => {
    const { task, lm_address } = req.body;

    if (!task || !lm_address) {
        return res.status(400).json({ error: 'Invalid task or LM address provided.' });
    }

    console.log('Task received:', task);
    console.log('LM Address:', lm_address);

    // Initialize Worker Node and Process Task
    const workerNode = new WorkerNode(lm_address);
    await workerNode.processTask(task);

    res.json({ status: 'Task received and processed.' });
});

// Start Server
const PORT = 18861;
app.listen(PORT, '0.0.0.0', () => {
    console.log(`Worker Node running on port ${PORT}`);
});

module.exports = WorkerNode;
