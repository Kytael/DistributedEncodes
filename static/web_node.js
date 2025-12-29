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
importScripts('ffmpeg.js');

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

    const inputFilename = "input_" + Date.now() + ".mp4";
    const outputFilename = "output_" + Date.now() + ".mp4";
    const inputPath = "/tmp/" + inputFilename;
    const outputPath = "/tmp/" + outputFilename;

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
        
        // Ensure /tmp exists
        try { FS.mkdir('/tmp'); } catch(e) {}
        
        FS.writeFile(inputPath, data);

        // DEBUG: Verify file existence
        try {
            const tmpFiles = FS.readdir('/tmp');
            postMessage({type: 'log', level: 'sys', msg: `DEBUG: /tmp contents: ${JSON.stringify(tmpFiles)}`});
            const cwd = FS.cwd();
            postMessage({type: 'log', level: 'sys', msg: `DEBUG: CWD: ${cwd}`});
            const stat = FS.stat(inputPath);
            postMessage({type: 'log', level: 'sys', msg: `DEBUG: Input file stat: ${stat.size} bytes`});
        } catch(e) {
            postMessage({type: 'log', level: 'err', msg: `DEBUG: File check failed: ${e.message}`});
        }
        
        // 3. Execute
        postMessage({type: 'log', level: 'sys', msg: "Starting FFmpeg..."});
        
        // SVT-AV1 Arguments
        const args = [
            '-threads', '1', 
            '-v', 'verbose',
            '-i', inputPath,
            '-c:v', 'libsvtav1',
            '-preset', '8',
            '-crf', '35',
            '-g', '240',
            '-pix_fmt', 'yuv420p10le', 
            '-svtav1-params', 'tune=0',
            '-c:a', 'libopus',
            '-b:a', '128k',
            '-ac', '1',
            outputPath
        ];

        // Wrap execution in a promise to wait for completion
        const ffmpegPromise = new Promise((resolve, reject) => {
            self.resolveJob = resolve;
            self.rejectJob = reject;
        });

        // FORCE MAIN THREAD EXECUTION
        // Bypass Emscripten's pthread proxying to ensure FFmpeg sees the local MEMFS
        if (Module["_main"] && Module["__emscripten_proxy_main"]) {
            Module["__emscripten_proxy_main"] = Module["_main"];
        }

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
        } catch(e) { exists = false; }

        if (!exists) {
            // Debug: List /tmp to see what happened
            try {
                postMessage({type: 'log', level: 'err', msg: `/tmp content: ${JSON.stringify(FS.readdir('/tmp'))}`});
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