import os
import sys
import platform
import subprocess
import urllib.request
import zipfile
import shutil
import time

# Direct download links for Windows standalone binaries
WIN_HB_URL = "https://github.com/HandBrake/HandBrake/releases/download/1.10.2/HandBrakeCLI-1.10.2-win-x86_64.zip"
# Gyan.dev is a trusted source for FFmpeg builds on Windows
WIN_FF_URL = "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip"

def print_step(msg):
    print(f"\n[+] {msg}")

def is_admin():
    try: return os.getuid() == 0
    except AttributeError:
        import ctypes
        return ctypes.windll.shell32.IsUserAnAdmin() != 0

def install_pip_deps():
    print_step("Checking Python Dependencies...")
    required = ["requests"]
    for package in required:
        try:
            __import__(package)
        except ImportError:
            print(f"    Installing {package}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])

def install_mac():
    print_step("Detected macOS System")
    if not shutil.which("brew"):
        print("   [!] Homebrew not found. Please install it first: https://brew.sh/")
        print("   [!] Or install HandBrakeCLI and FFmpeg manually.")
        return
    
    print_step("Installing HandBrakeCLI & FFmpeg via Homebrew...")
    try: 
        subprocess.check_call(["brew", "install", "handbrake", "ffmpeg"])
    except subprocess.CalledProcessError: 
        print("   [!] Error installing via Brew. You may need to run this manually.")
        return
        
    install_pip_deps()
    print_step("macOS Setup Complete!")

def install_linux():
    print_step("Detected Linux System")
    
    # Detect Package Manager
    if shutil.which("apt-get"):
        pkg_mgr = "apt-get"
        # Ubuntu/Debian often separate ffmpeg and handbrake-cli
        pkgs = ["handbrake-cli", "ffmpeg", "python3-requests"]
    elif shutil.which("dnf"):
        pkg_mgr = "dnf"
        pkgs = ["HandBrake-cli", "ffmpeg", "python3-requests"]
    elif shutil.which("pacman"):
        pkg_mgr = "pacman"
        pkgs = ["handbrake-cli", "ffmpeg", "python-requests"]
    else:
        print("   [!] Error: Supported package manager (apt/dnf/pacman) not found.")
        print("   [!] Please install HandBrakeCLI and FFmpeg manually.")
        return

    # Check Root
    if not is_admin():
        print(f"   [!] Linux setup requires root permissions to install packages.")
        print(f"   [!] Rerunning with sudo...")
        try:
            os.execvp("sudo", ["sudo", sys.executable] + sys.argv)
        except Exception as e:
            print(f"   [!] Failed to elevate: {e}")
            return

    # Install
    print_step(f"Updating {pkg_mgr} repositories...")
    update_flag = "update" if pkg_mgr != "pacman" else "-Sy"
    subprocess.call([pkg_mgr, update_flag])
    
    print_step(f"Installing packages: {', '.join(pkgs)}...")
    install_args = [pkg_mgr, "install", "-y" if pkg_mgr != "pacman" else "-S"]
    if pkg_mgr == "pacman": install_args.append("--noconfirm")
    install_args.extend(pkgs)
    
    subprocess.call(install_args)
    print_step("Linux Setup Complete!")

def install_windows():
    print_step("Detected Windows System")
    
    # 1. Install HandBrakeCLI
    if not os.path.exists("HandBrakeCLI.exe"):
        print_step("Downloading HandBrakeCLI...")
        try:
            with urllib.request.urlopen(WIN_HB_URL) as r, open("hb.zip", 'wb') as f:
                shutil.copyfileobj(r, f)
            
            with zipfile.ZipFile("hb.zip", 'r') as z:
                # HandBrake zip usually has the exe at the root
                if "HandBrakeCLI.exe" in z.namelist():
                    with open("HandBrakeCLI.exe", "wb") as f:
                        f.write(z.read("HandBrakeCLI.exe"))
            
            os.remove("hb.zip")
            print("   [OK] HandBrakeCLI.exe installed.")
        except Exception as e:
            print(f"   [!] Failed to download HandBrake: {e}")

    # 2. Install FFmpeg & FFprobe
    if not os.path.exists("ffmpeg.exe") or not os.path.exists("ffprobe.exe"):
        print_step("Downloading FFmpeg & FFprobe (This may take a moment)...")
        try:
            with urllib.request.urlopen(WIN_FF_URL) as r, open("ff.zip", 'wb') as f:
                shutil.copyfileobj(r, f)
            
            with zipfile.ZipFile("ff.zip", 'r') as z:
                # FFmpeg zip has nested folders (e.g. ffmpeg-6.0-essentials_build/bin/ffmpeg.exe)
                # We need to find the paths dynamically
                for name in z.namelist():
                    if name.endswith("bin/ffmpeg.exe"):
                        with open("ffmpeg.exe", "wb") as f: f.write(z.read(name))
                    elif name.endswith("bin/ffprobe.exe"):
                        with open("ffprobe.exe", "wb") as f: f.write(z.read(name))
            
            os.remove("ff.zip")
            print("   [OK] FFmpeg tools installed.")
        except Exception as e:
            print(f"   [!] Failed to download FFmpeg: {e}")

    install_pip_deps()
    print_step("Windows Setup Complete!")

def main():
    print(":: FRACTUM DISTRIBUTED ENCODES WORKER SETUP ::")
    print("This script will download the required encoders and dependencies.")
    
    sys_os = platform.system()
    if sys_os == "Linux": install_linux()
    elif sys_os == "Windows": install_windows()
    elif sys_os == "Darwin": install_mac()
    else: print(f"Unsupported OS: {sys_os}")
    
    print("\n[!] Setup finished.")
    if sys_os == "Windows":
        input("Press Enter to exit...")

if __name__ == "__main__": 
    main()
