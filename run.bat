@echo off 
echo Starting Map_researcher0.3V... 
 
if exist venv\Scripts\python.exe ( 
   venv\Scripts\python main.py %* 
) else ( 
   python main.py %* 
) 
