📹 yt-shorts

🚀 A lightweight web app to run and manage YouTube Shorts processing jobs with parallel execution and real-time tracking.

✨ Features
🎬 Run Shorts processing jobs
⚡ Parallel execution (multiprocessing)
📊 Live job status tracking
🛑 Kill running jobs from UI
📁 Structure
```
app.py                # Flask backend
shorts_pipeline.py   # Processing pipeline
templates/           # Frontend
```
⚙️ Setup
```
git clone https://github.com/abhay31-dev/yt-shorts.git
cd yt-shorts

python3 -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate

pip install -r requirements.txt
```
▶️ Run
```
python app.py

🌐 http://localhost:5000
```

⚡ Flow
UI → Flask → Multiprocessing → Pipeline → Status
👨‍💻 Author

Abhay Kumar
🔗 https://github.com/abhay31-dev
