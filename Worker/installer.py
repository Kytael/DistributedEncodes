import os
import sys
import platform
import subprocess
import urllib.request
import zipfile
import shutil

# --- CONFIGURATION ---
WIN_HB_URL = "https://github.com/HandBrake/HandBrake/releases/download/1.10.2/HandBrakeCLI-1.10.2-win-x86_64.zip"
WIN_FF_URL = "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip"

def print_step(msg):
    print(f"\n[+] {msg}")

def is_admin():
    try:
        return os.getuid() == 0
    except AttributeError:
        import ctypes
        return ctypes.windll.shell32.IsUserAnAdmin() != 0

def install_pip_deps_windows():
    """Windows Only: Installs requests via pip"""
    print_step("Checking Python Dependencies (Windows)...")
    try:
        __import__("requests")
        print("   [OK] requests is already installed.")
    except ImportError:
        print("   Installing requests via pip...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "requests"])
            print("   [SUCCESS] Installed requests.")
        except subprocess.CalledProcessError:
            print("   [ERROR] Failed to install requests.")

def install_linux():
    print_step("Detected Linux System")
    
    # Determine Package Manager & Package Names
    # Include python3-requests directly in the system install list
    if shutil.which("apt-get"):
        pkg_manager = "apt-get"
        # Debian/Ubuntu/Raspberry Pi
        packages = ["handbrake-cli", "ffmpeg", "python3-requests"]
        update_cmd = [pkg_manager, "update"]
        install_cmd = [pkg_manager, "install", "-y"] + packages
        
    elif shutil.which("dnf"):
        pkg_manager = "dnf"
        # Fedora/RHEL
        packages = ["HandBrake-cli", "ffmpeg", "python3-requests"]
        update_cmd = [pkg_manager, "check-update"]
        install_cmd = [pkg_manager, "install", "-y"] + packages
        
    elif shutil.which("pacman"):
        pkg_manager = "pacman"
        # Arch Linux (Arch uses 'python-requests')
        packages = ["handbrake-cli", "ffmpeg", "python-requests"]
        update_cmd = [pkg_manager, "-Sy"]
        install_cmd = [pkg_manager, "-S", "--noconfirm"] + packages
        
    else:
        print("Error: Could not detect apt, dnf, or pacman. Please install tools manually.")
        return

    # Check Root
    if not is_admin():
        print(f"!! Linux setup requires root to install {packages}. Rerunning with sudo...")
        try:
            os.execvp("sudo", ["sudo", sys.executable] + sys.argv)
        except Exception as e:
            print(f"Failed to elevate privileges: {e}")
            return

    # Install
    print_step(f"Installing System Tools & Python Libs via {pkg_manager}...")
    print(f"   Targets: {', '.join(packages)}")
    
    # Run Update
    subprocess.call(update_cmd)
    
    # Run Install
    ret = subprocess.call(install_cmd)
    
    if ret == 0:
        print_step("Linux Setup Complete!")
    else:
        print_step("Installation failed. Please check your internet or package manager.")

def install_windows():
    print_step("Detected Windows System")

    # 1. HandBrakeCLI
    if not os.path.exists("HandBrakeCLI.exe"):
        print_step("Downloading HandBrakeCLI...")
        hb_zip = "handbrake.zip"
        try:
            req = urllib.request.Request(WIN_HB_URL, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req) as response, open(hb_zip, 'wb') as out_file:
                shutil.copyfileobj(response, out_file)
            print("   Extracting...")
            with zipfile.ZipFile(hb_zip, 'r') as z:
                for name in z.namelist():
                    if name.endswith("HandBrakeCLI.exe"):
                        with open("HandBrakeCLI.exe", "wb") as f: f.write(z.read(name))
            os.remove(hb_zip)
            print("   HandBrakeCLI installed.")
        except Exception as e: print(f"   Error: {e}")
    else: print("   HandBrakeCLI already exists.")

    # 2. FFprobe
    if not os.path.exists("ffprobe.exe"):
        print_step("Downloading FFprobe...")
        ff_zip = "ffmpeg.zip"
        try:
            req = urllib.request.Request(WIN_FF_URL, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req) as response, open(ff_zip, 'wb') as out_file:
                shutil.copyfileobj(response, out_file)
            print("   Extracting...")
            with zipfile.ZipFile(ff_zip, 'r') as z:
                for name in z.namelist():
                    if name.endswith("ffprobe.exe"):
                        with open("ffprobe.exe", "wb") as f: f.write(z.read(name))
            os.remove(ff_zip)
            print("   FFprobe installed.")
        except Exception as e: print(f"   Error: {e}")
    else: print("   FFprobe already exists.")

    # 3. Python Deps (Pip is okay on Windows)
    install_pip_deps_windows()

    print_step("Windows Setup Complete!")

def main():
    print(":: FRACTUM DISTRIBUTED ENCODES WORKER SETUP ::")
    sys_os = platform.system()
    
    if sys_os == "Linux":
        install_linux()
    elif sys_os == "Windows":
        install_windows()
    else:
        print(f"Unsupported OS: {sys_os}")
        
    # Only pause on Windows so the window doesn't close immediately
    if sys_os == "Windows":
        input("\nPress Enter to exit...")

if __name__ == "__main__":
    main()