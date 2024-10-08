const jwt = require('jsonwebtoken');

const WebSocket = require('ws');
const websocketManager = require('./websocket-manager'); // Import the WebSocket manager

const express = require('express');
const { Worker, Queue } = require('bullmq');
const Redis = require('ioredis');
const { getSchedulerJobByID, runSchedulerJob, runRevertSchedulerJob } = require('./scheduler-functions-for-redis');

const app = express();
const rateLimit = require('express-rate-limit');

// Trust proxy (needed for rate limiting with proxies)
app.set('trust proxy', 1);

// Middleware to parse JSON body
app.use(express.json());


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

const verifyToken = (req, res, next) => {
    const token = req.headers['authorization']?.split(' ')[1];

    if (!token) {
        return res.status(403).send('Token is required');
    }

    let secretKey = process.env.SECRET_KEY;
    jwt.verify(token, secretKey, (err, decoded) => {
        if (err) {
            return res.status(401).send('Invalid token');
        }
        req.sessionData = decoded; // Attach session data to request
        next();
    });
};


app.get('/', (req, res) => {
    runScheduledUpdateTask(408);  // Run the product update task
    res.send('Hello, World!');
});


app.post('/', verifyToken ,async (req, res) => {
    
    try {

        const { scheduler_id, type } = req.body;

        if(scheduler_id && type){
            if (type == 'scheduleUpdateJob') {
                console.log(`Manually Processing scheduled update for job ${scheduler_id} at ${new Date().toISOString()}`);
                await runScheduledUpdateTask(scheduler_id);  // Run the product update task
            }
    
            if (type == 'revertUpdateJob') {
                console.log(`Manually Processing revert for job ${scheduler_id} at ${new Date().toISOString()}`);
                await runRevertUpdateTask(scheduler_id);  // Run the revert task
            }
            res.send(`Scheduled update task for scheduler ID: ${scheduler_id} has been triggered!`);
        }
    } catch (error) {
        // Make sure not to call res.send again here
        console.error(error);
        res.status(500).send('Error occurred');
    }
});



// app.listen(PORT, () => {
//   console.log(`Server is running on port ${PORT}`);
// });

// Start Express server
const server = app.listen(PORT || 3000, () => {
    console.log(`Server is running on port ${PORT || 3000}`);
});

// WebSocket Server, using the same HTTP server
const wss = new WebSocket.Server({ server });
wss.on('connection', (ws, req) => {
    const shop = req.url.split('?shop=')[1];  // Assume you pass the shop in the URL
    console.log(`Client connected for shop: ${shop}`);
    
    // Add the client to the manager
    websocketManager.addClient(shop, ws);

    ws.on('message', (message) => {
        console.log(`Received message from shop ${shop}: ${message}`);
    });

    ws.on('close', () => {
        websocketManager.removeClient(shop); // Remove the client on disconnection
        console.log(`Client for shop ${shop} disconnected`);
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
