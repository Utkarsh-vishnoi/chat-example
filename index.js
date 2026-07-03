import cluster from 'cluster';
import os from 'os';
import http from 'http';
import express from 'express';
import { Server } from 'socket.io';
import sqlite3 from 'sqlite3';
import clusterAdapter from '@socket.io/cluster-adapter';

const { setupPrimary, createAdapter } = clusterAdapter;

// Configuration
const BASE_PORT = 3000;
const MAX_FILE_SIZE = 50 * 1024 * 1024; // 50 MB limit
const sqlite = sqlite3.verbose();

if (cluster.isPrimary) {
    // 1. Route messages between different ports/workers
    setupPrimary();

    // 2. ONE-TIME DATABASE INITIALIZATION
    // This guarantees the DB and tables are created before workers start
    const db = new sqlite.Database('./chat.db');
    
    db.serialize(() => {
        db.run("PRAGMA journal_mode = WAL;"); 
        
        db.run(`CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            type TEXT, 
            content TEXT, 
            filename TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )`, (err) => {
            if (err) {
                console.error("Database initialization failed:", err);
                process.exit(1);
            }
            
            // 3. Database is ready, now fork the workers
            const numCPUs = os.cpus().length;
            console.log(`Primary process ${process.pid} is running.`);
            console.log(`Database verified. Forking ${numCPUs} workers on multiple ports...`);
            
            // Note: Starting loop at 0 so first port is exactly BASE_PORT (3000)
            for (let i = 0; i < numCPUs; i++) {
                cluster.fork({ WORKER_ID: i });
            }
        });
    });

    // Auto-heal dead workers
    cluster.on('exit', (worker, code, signal) => {
        console.log(`Worker ${worker.process.pid} died. Restarting...`);
        cluster.fork({ WORKER_ID: worker.id }); 
    });

} else {
    // --- WORKER PROCESS ---
    const workerId = parseInt(process.env.WORKER_ID, 10);
    const port = BASE_PORT + workerId; // e.g., 3000, 3001, 3002...
    
    const app = express();
    const server = http.createServer(app);
    
    // Initialize Socket.io with the 50MB limit AND the cluster adapter
    const io = new Server(server, {
        maxHttpBufferSize: MAX_FILE_SIZE,
        adapter: createAdapter() 
    });

    // Connect this specific worker to the already-initialized DB
    const db = new sqlite.Database('./chat.db');

    // Serve Frontend
    app.use(express.static('public'));

    io.on('connection', (socket) => {
        console.log(`User connected on port ${port} (Worker PID: ${process.pid})`);

        // Send chat history on connect
        db.all("SELECT * FROM messages ORDER BY timestamp ASC", [], (err, rows) => {
            if (!err) socket.emit('history', rows);
        });

        // Handle Text Messages
        socket.on('text-message', (text) => {
            db.run("INSERT INTO messages (type, content) VALUES ('text', ?)", [text], (err) => {
                if (err) console.error("DB Insert Error:", err.message);
            });
            io.emit('text-message', { type: 'text', content: text });
        });

        // Handle Binary File Transfers
        socket.on('file-transfer', (fileObj) => {
            // Log file metadata to SQLite
            const logMsg = `Transferred file: ${fileObj.name}`;
            db.run("INSERT INTO messages (type, content, filename) VALUES ('file', ?, ?)", [logMsg, fileObj.name], (err) => {
                if (err) console.error("DB Insert Error:", err.message);
            });
            
            // Broadcast the raw binary to EVERYONE ELSE across all ports
            socket.broadcast.emit('file-transfer', fileObj);
            
            // Send a text notification to the chat UI
            io.emit('text-message', { type: 'system', content: logMsg });
        });
    });

    server.listen(port, () => {
        console.log(`Worker ${workerId} (PID: ${process.pid}) listening on http://localhost:${port}`);
    });
}
