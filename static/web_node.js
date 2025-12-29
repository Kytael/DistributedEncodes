// static/web_node.js
// FRACTUM Web Node Worker

// Calculate base path for loading assets
const basePath = self.location.href.substring(0, self.location.href.lastIndexOf('/'));

self.Module = {
    print: function(text) { postMessage({type: 'log', level: 'sys', msg: "STDOUT: " + text}); },
    printErr: function(text) { postMessage({type: 'log', level: 'err', msg: "STDERR: " + text}); },
    onRuntimeInitialized: function() {
        // Only signal ready if FS is actually available
        if (self.Module.FS || self.FS) {
            postMessage({type: 'ready'});
        } else {
            postMessage({type: 'log', level: 'err', msg: "Runtime initialized but FS missing."});
        }
    },
    preRun: [function() {
        const FS = self.Module.FS || self.FS;
        try {
            FS.mkdir('/work');
        } catch(e) { console.log("Mkdir error (might exist): " + e); }
        
        try {
            FS.mount(self.Module.MEMFS, {}, '/work');
        } catch(e) { console.log("Mount error (might be mounted): " + e); }
    }],
    // CRITICAL: Point Pthreads to the actual Emscripten JS file, NOT this worker wrapper.
    // Otherwise, Pthreads import web_node.js -> import ffmpeg.js -> infinite loop or broken context.
    mainScriptUrlOrBlob: basePath + "/ffmpeg.js",
    noInitialRun: true,
    noExitRuntime: true,
    // We handle exit manually to detect job completion
    quit: function(status, toThrow) {
        postMessage({type: 'log', level: 'sys', msg: `FFmpeg exit with status ${status}`});
        if (self.resolveJob) {
            if (status === 0) self.resolveJob();
            else self.rejectJob(new Error(`FFmpeg exited with status ${status}`));
            self.resolveJob = null;
            self.rejectJob = null;
        }
        // Emscripten expects quit to throw to stop execution
        if (toThrow) throw toThrow;
    },
};

// Load FFmpeg WASM
importScripts('ffmpeg.js?v=' + Date.now());

// Capture Emscripten's message handler (if any) to preserve Pthread communication
const emscriptenOnMessage = self.onmessage;

self.onmessage = async function(e) {
    const msg = e.data;
    
    // Handle our custom job messages
    if (msg && msg.type === 'run_job') {
        try {
            await processJob(msg.job);
        } catch (err) {
            postMessage({type: 'error', msg: err.toString()});
        }
        return; // Don't pass job messages to Emscripten
    }

    // Pass everything else (e.g. Pthread messages) to Emscripten
    if (emscriptenOnMessage) {
        emscriptenOnMessage(e);
    }
};

async function processJob(job) {
    // FS and callMain might be on Module or global scope depending on Emscripten build options
    const FS = self.Module.FS || self.FS;
    const callMain = self.Module.callMain || self.callMain;
    
    if (!FS || !callMain) {
        throw new Error(`FFmpeg primitives missing. FS: ${!!FS}, callMain: ${!!callMain}`);
    }

    // Use /work directory to avoid /tmp mount issues and ensure clean state
    const inputFilename = "input.mp4";
    const outputFilename = "output.mp4";
    const inputPath = "/work/" + inputFilename;
    const outputPath = "/work/" + outputFilename;

    postMessage({type: 'log', level: 'sys', msg: `Worker processing: ${job.filename}`});

    try {
        // 1. Download
        postMessage({type: 'log', level: 'sys', msg: "Downloading source..."});
        const resp = await fetch(job.download_url);
        if (!resp.ok) throw new Error("Download failed: " + resp.status);
        
        const buf = await resp.arrayBuffer();
        const data = new Uint8Array(buf);
        
        // 2. Write to MEMFS
        postMessage({type: 'log', level: 'sys', msg: `Writing ${data.length} bytes to ${inputPath}`});
        
        // Cleanup previous runs just in case
        try { FS.unlink(inputPath); } catch(e) {}
        try { FS.unlink(outputPath); } catch(e) {}
        
        FS.writeFile(inputPath, data);
        
        // Verify input exists
        try {
            const stat = FS.stat(inputPath);
            postMessage({type: 'log', level: 'sys', msg: `Input file verified on FS: ${stat.size} bytes`});
        } catch(e) {
            throw new Error(`Failed to verify input file at ${inputPath} after write.`);
        }
        
        // 3. Execute
        postMessage({type: 'log', level: 'sys', msg: "Starting FFmpeg..."});
        
        // STRICT ENCODING CONFIGURATION
        const args = [
            '-threads', '1', 
            '-v', 'verbose',
            '-i', inputPath,
            '-c:v', 'libsvtav1',
            '-preset', '2',
            '-crf', '63',
            '-g', '240',
            '-pix_fmt', 'yuv420p', 
            '-svtav1-params', 'tune=0',
            '-vf', 'scale=-2:480',
            '-c:a', 'libopus',
            '-b:a', '12k',
            '-ac', '1',
            '-c:s', 'mov_text',
            outputPath
        ];

        // Wrap execution in a promise to wait for completion
        const ffmpegPromise = new Promise((resolve, reject) => {
            self.resolveJob = resolve;
            self.rejectJob = reject;
        });

        // callMain might return immediately if proxied, or block. 
        // We await our custom promise which is triggered by Module.quit
        callMain(args);
        
        await ffmpegPromise;

        // 4. Read Output
        postMessage({type: 'log', level: 'sys', msg: "Reading output..."});
        
        // Verify output exists
        let exists = false;
        try {
            const stat = FS.stat(outputPath);
            exists = true;
            postMessage({type: 'log', level: 'sys', msg: `Output file size: ${stat.size} bytes`});
        } catch(e) { exists = false; }

        if (!exists) {
            // Debug: List /work to see what happened
            try {
                postMessage({type: 'log', level: 'err', msg: `Work dir content: ${JSON.stringify(FS.readdir('/work'))}`});
            } catch(e){}
            throw new Error("FFmpeg did not create output file (check logs for errors).");
        }
        
        const outData = FS.readFile(outputPath);
        const blob = new Blob([outData], { type: 'video/mp4' });
        
        // 5. Upload
        postMessage({type: 'log', level: 'sys', msg: `Uploading ${outData.length} bytes...`});
        
        const formData = new FormData();
        formData.append('job_id', job.id);
        formData.append('worker_id', job.worker_id);
        formData.append('file', blob, 'result.mp4');

        const up = await fetch('/upload_result', { method: 'POST', body: formData });
        if (!up.ok) throw new Error("Upload failed: " + up.statusText);
        
        postMessage({type: 'done'});

    } catch (e) {
        throw e;
    } finally {
        // Cleanup
        try {
            if (FS) {
                try { FS.unlink(inputPath); } catch(e) {}
                try { FS.unlink(outputPath); } catch(e) {}
            }
        } catch (e) {
            postMessage({type: 'log', level: 'err', msg: "Cleanup error: " + e.message});
        }
    }
}