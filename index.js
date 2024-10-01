const WebSocket = require('ws');

const express = require('express');
const { Worker, Queue } = require('bullmq');
const Redis = require('ioredis');
const { getSchedulerJobByID, runSchedulerJob, runRevertSchedulerJob } = require('./scheduler-functions-for-redis');

const app = express();
const rateLimit = require('express-rate-limit');



// require('./schedulers');
// require('./revert-scheduler');


// Set up a rate limiter
const limiter = rateLimit({
    windowMs: 1 * 60 * 1000, // 1 minute
    max: 100, // Limit each IP to 100 requests per windowMs
    message: 'Too many requests, please try again later.',
});


const PORT = process.env.PORT || 3000;

// Apply the rate limiter to all requests
app.use(limiter);

app.get('/', (req, res) => {
  res.send('Hello, World!');
});

// app.listen(PORT, () => {
//   console.log(`Server is running on port ${PORT}`);
// });

// Start Express server
const server = app.listen(PORT || 3000, () => {
    console.log(`Server is running on port ${PORT || 3000}`);
});

// WebSocket Server, using the same HTTP server

const clients = new Map();

const wss = new WebSocket.Server({ server });

wss.on('connection', (ws, req) => {
    const urlParams = new URLSearchParams(req.url.split('?')[1]);
    console.log("urlParams urlParams urlParams", urlParams);

    const shop = urlParams.get('shop'); // Extract shop identifier from query parameters

    console.log("shop shop shop", shop);
    clients.set(shop, ws); // Store the connection

    ws.on('message', (message) => {


        // const { action, data } = JSON.parse(message);

        console.log("message message message", message);

        console.log(`Received message from ${shop}: ${message}`);
        // Handle messages for this specific client here
    });

    ws.send(`Hello, you are connected with Shop: ${shop}`);

    ws.on('close', (code, reason) => {
        clients.delete(shop); // Clean up when the client disconnects
        // console.log('Client disconnected');
        console.log(`Client for shop ${shop} disconnected. Close code: ${code}, Reason: ${reason}`);
    });


    ws.on('error', (error) => {
        console.error(`WebSocket error for ${shop}:`, error);
    });
});





// Redis connection
const redisConnectionUrl = process.env.REDIS_URL || 'rediss://default:VJktXv52bNTZZB6ilOdY4ojkruZqmHwUv6mAJccd1CDrurowRQOFmkca8GLikizq@so89sw.stackhero-network.com:10008';
const redisOptions = {
    maxRetriesPerRequest: null, // Required by BullMQ
};


console.log("redisConnectionUrl:: ",redisConnectionUrl);
const redis = new Redis(redisConnectionUrl, redisOptions);

// Create the worker to process both 'scheduleUpdateJob' and 'revertUpdateJob'
const worker = new Worker(
    'scheduler',
    async job => {
        const { jobId } = job.data;
        
        if (job.name == 'scheduleUpdateJob') {
            console.log(`Processing scheduled update for job ${jobId} at ${new Date().toISOString()}`);
            await runScheduledUpdateTask(jobId);  // Run the product update task
        }

        if (job.name == 'revertUpdateJob') {
            console.log(`Processing revert for job ${jobId} at ${new Date().toISOString()}`);
            await runRevertUpdateTask(jobId);  // Run the revert task
        }
    },
    { connection: redis }
);

// Worker error event
worker.on('error', (err) => {
    console.error('Worker encountered an error:', err);
});

// Worker ready event (to know when the worker is ready to process jobs)
worker.on('ready', () => {
    console.log('Worker is ready and connected to Redis');
});

// Worker event when job starts processing
worker.on('active', (job) => {
    console.log(`Job ${job.id} is now active and being processed`);
});

// Event listener for completed jobs
worker.on('completed', (job) => {
    console.log(`Job ${job.id} (${job.name}) completed successfully.`);
});

// Event listener for failed jobs
worker.on('failed', (job, err) => {
    console.error(`Job ${job.id} (${job.name}) failed with error: ${err.message}`);
});


// Redis error event
redis.on('error', (err) => {
    console.error('Redis connection error:', err);
});

// Redis connect event
redis.on('connect', () => {
    console.log('Connected to Redis successfully');
});

// Redis reconnecting event
redis.on('reconnecting', (delay) => {
    console.log(`Reconnecting to Redis, next attempt in ${delay}ms`);
});

// Redis end event
redis.on('end', () => {
    console.log('Redis connection closed');
});



const queue = new Queue('scheduler', { connection: redis });

queue.on('error', (err) => {
    console.error('Queue encountered an error:', err);
});

queue.on('waiting', (jobId) => {
    console.log(`Job ${jobId} is waiting to be processed`);
});

queue.on('active', (job) => {
    console.log(`Job ${job.id} is now active`);
});

queue.on('completed', (job) => {
    console.log(`Job ${job.id} completed successfully`);
});

queue.on('failed', (job, err) => {
    console.error(`Job ${job.id} failed with error: ${err.message}`);
});


// Your task logic (replace these with actual tasks)
async function runScheduledUpdateTask(jobId) {
    let data = await getSchedulerJobByID(jobId);
    console.log(`data for JobId = ${jobId} ----->>>> runScheduledUpdateTask(): `, data);
    if(data?.length > 0){
        let session = {
            shop: data[0]?.shop_name,
            accessToken: data[0]?.access_token
        };        
        await runSchedulerJob(session, jobId);
    }
    console.log(`Running scheduled product update for job ${jobId}`);
}

async function runRevertUpdateTask(jobId) {
    let data = await getSchedulerJobByID(jobId);
    console.log(`data for JobId = ${jobId} ----->>>> runRevertUpdateTask(): `, data);
    if(data?.length > 0){
        let session = {
            shop: data[0]?.shop_name,
            accessToken: data[0]?.access_token
        };        
        await runRevertSchedulerJob(session, jobId);
    }
    console.log(`Running revert product update for job ${jobId}`);
}



// require('dotenv').config();
