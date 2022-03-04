# socialHealth
This is a project to monitor and analyze company's social medias posts/comments.
The backend is using Python and Jupyter Python notebook to download data from Facebook & Twitter and populate anlytics result. Data and result is saved in mySQL database. The front end is using Python Flask for user interaction and visualization.

File and Folder structure:
app.py is the main Flask web application file. templates and static folder is for Flask web application to load pictures and web pages. 

fb_download_complete_sql.py and twitter_download_complete_sql.py is the main functions to download data from Facebook and Twitter and save data into a MySQL database

AnalysisEngine.ipynb is a Jupyter iPython notebook for generating analytics result and visualziation based on raw Facebook and Twitter data
