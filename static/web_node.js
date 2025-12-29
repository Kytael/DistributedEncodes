// static/web_node.js
// FRACTUM Web Node Worker

self.Module = {
    print: function(text) { postMessage({type: 'log', level: 'sys', msg: "STDOUT: " + text}); },
    printErr: function(text) { postMessage({type: 'log', level: 'err', msg: "STDERR: " + text}); },
    onRuntimeInitialized: function() {
        postMessage({type: 'ready'});
    }
};

// Load FFmpeg WASM
importScripts('ffmpeg.js');

self.onmessage = async function(e) {
    const msg = e.data;
    if (msg.type === 'run_job') {
        try {
            await processJob(msg.job);
        } catch (err) {
            postMessage({type: 'error', msg: err.toString()});
        }
    }
};

async function processJob(job) {
    const FS = self.Module.FS;
    const callMain = self.Module.callMain;
    
    if (!FS || !callMain) {
        throw new Error("FFmpeg not initialized");
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
        
        // Free buffer memory
        // data = null; // Can't null const, but function scope handles it? No, explicit helps.
        // We need 'data' to pass to writeFile, but after that we don't need it.
        // JS GC should handle it, but we are tight on memory.
        
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

        callMain(args);

        // 4. Read Output
        postMessage({type: 'log', level: 'sys', msg: "Reading output..."});
        if (!FS.analyzePath(outputPath).exists) {
            throw new Error("FFmpeg did not create output file");
        }
        
        const outData = FS.readFile(outputPath);
        const blob = new Blob([outData], { type: 'video/mp4' });
        
        // 5. Upload
        postMessage({type: 'log', level: 'sys', msg: `Uploading ${outData.length} bytes...`});
        
        const formData = new FormData();
        formData.append('job_id', job.id);
        formData.append('worker_id', job.worker_id); // Passed from main
        formData.append('file', blob, 'result.mp4');

        const up = await fetch('/upload_result', { method: 'POST', body: formData });
        if (!up.ok) throw new Error("Upload failed: " + up.statusText);
        
        postMessage({type: 'done'});

    } catch (e) {
        throw e;
    } finally {
        // Cleanup
        try {
            if (FS.analyzePath(inputPath).exists) FS.unlink(inputPath);
            if (FS.analyzePath(outputPath).exists) FS.unlink(outputPath);
        } catch (e) {
            postMessage({type: 'log', level: 'err', msg: "Cleanup error: " + e.message});
        }
    }
}
