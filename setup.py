#!/usr/bin/env python3

import os
import sys
import subprocess

def install_packages():
    print("Installing required packages...")
    try:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'])
        print("✓ Packages installed successfully")
        return True
    except subprocess.CalledProcessError:
        print("✗ Failed to install packages")
        return False

def setup_config():
    print("Setting up configuration...")
    
    if not os.path.exists('.env'):
        if os.path.exists('.env.example'):
            print("Creating .env from .env.example...")
            with open('.env.example', 'r') as src, open('.env', 'w') as dst:
                dst.write(src.read())
            print("✓ .env file created")
            print("  → Please edit .env and add your OpenWeatherMap API key")
        else:
            print("✗ .env.example not found")
            return False
    else:
        print("✓ .env file already exists")
    
    # Create data directory
    os.makedirs('data', exist_ok=True)
    print("✓ Data directory created")
    
    # Create logs directory
    os.makedirs('logs', exist_ok=True)
    print("✓ Logs directory created")
    
    return True

def run_demo():
    """Run the demo pipeline"""
    print("Running demo pipeline...")
    try:
        result = subprocess.run([sys.executable, 'demo_weather_etl.py'], 
                              input='1\n', text=True, capture_output=True)
        if result.returncode == 0:
            print("✓ Demo pipeline completed successfully")
            return True
        else:
            print("✗ Demo pipeline failed")
            print(result.stderr)
            return False
    except Exception as e:
        print(f"✗ Error running demo: {e}")
        return False

def check_api_setup():
    """Check if API is properly configured"""
    print("Checking API configuration...")
    
    if not os.path.exists('.env'):
        print("✗ .env file not found")
        return False
    
    with open('.env', 'r') as f:
        content = f.read()
    
    if 'your_api_key_here' in content:
        print("⚠ API key not configured - please edit .env file")
        return False
    
    if 'OPENWEATHER_API_KEY=' in content:
        lines = content.split('\n')
        for line in lines:
            if line.startswith('OPENWEATHER_API_KEY=') and len(line.split('=')[1].strip()) > 10:
                print("✓ API key appears to be configured")
                return True
    
    print("⚠ API key may not be properly configured")
    return False

def main():
    """Main setup function"""
    print("=== Weather ETL Pipeline Setup ===\n")
    
    success = True
    
    # Install packages
    if not install_packages():
        success = False
    
    print()
    
    # Setup configuration
    if not setup_config():
        success = False
    
    print()
    
    # Run demo
    if not run_demo():
        success = False
    
    print()
    
    # Check API setup
    api_ready = check_api_setup()
    
    print("\n=== Setup Summary ===")
    
    if success:
        print("✓ Basic setup completed successfully")
        
        if api_ready:
            print("✓ API configuration looks good")
            print("\nYou can now run the weather ETL pipeline:")
            print("  python etl-pipeline.py")
        else:
            print("⚠ Please configure your API key in .env file")
            print("\nSteps to complete setup:")
            print("1. Get API key from: https://openweathermap.org/api")
            print("2. Edit .env file and replace 'your_api_key_here' with your actual API key")
            print("3. Run: python etl-pipeline.py")
    else:
        print("✗ Setup encountered errors")
        print("Please check the error messages above and try again")
    
    print("\nFor help, see README.md or run:")
    print("  python demo_weather_etl.py")

if __name__ == "__main__":
    main()
