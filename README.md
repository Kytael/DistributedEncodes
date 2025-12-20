# FractumSeraphs Distributed Encodes

This tool is a distributed system that lets you crowd-source video transcoding. It splits the workload across multiple "Worker" computers (friends/volunteers/extra PCs), coordinates them via a central Manager, and uses a FTP server for file transfer. It's split into four main parts.
manager.py: A Python/Flask server that tracks jobs, users, and the leaderboard.
docker-compose.yml: A Dockerized FTP server.
worker.py: A standalone client (Windows/Linux) that downloads, transcodes, and uploads videos.
populate.py: Scans your local library to create jobs for the manager to hand out to workers.

You will need a PC to use as the server, so it will need to have some ports forwarded. By default, ports 21, 5000, and 30000-30100 are used.
You'll also need python3 and pip.
In the docker-compose file you'll need to change the address to that of your server.
By deault the username and password of your ftp server are both 'transcode'.

For the manager.py file, you'll need to set an API secret key. This is essentially a password and you'll need to put the same key in the other files as well.
To run the manager you'll need to install flask and flask-cors with pip.
'pip install flask flask-cors'

In the populate.py file, you need to change your server address, the API from before, and the location of your media.

For the worker.py you'll need to set your server address(manager.py), the API key again, and your FTP server details.
For workers, rather than requiring people to install python, you can instead use pyinstaller to create a .exe.
'pip install pyinstaller'
'pyinstaller --onefile --name WorkerWithoutPython worker.py'
Now you can take the WorkerWithoutPython.exe file (rename it if you'd like) and place it in a folder with HandbrakeCLI, Your preset.json, and ffprobe.exe Sip up the folder and send it to your workers.

For the stats dashboard, edit the API_URL to point to your manager.py server.

For your media files, ensure they match this structure:
/Media_Root/
    |
    |--- source/          <-- Original videos
    |      |--- Series A/
    |      |--- Movie.mkv
    |
    |--- completed/       <-- Uploaded encoded versions get put here
