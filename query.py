from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import os

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', 'postgres://postgres:password@db:5432/postgres')
db = SQLAlchemy(app)

class UserExperimentStats(db.Model):
    __tablename__ = 'user_experiment_stats'
    user_id = db.Column(db.Integer, primary_key=True)
    total_experiments = db.Column(db.Integer)
    avg_experiments = db.Column(db.Float)
    max_freq_compound = db.Column(db.String)

#Query DB table
def query_table():
    #Fetch all entries
    rows = UserExperimentStats.query.all()

    if not rows:
        print("Warning: The table is empty.")

    for row in rows:
        print(f"User ID: {row.user_id}")
        print(f"Total no. of experiments: {row.total_experiments}")
        print(f"Avg no. of experiments: {row.avg_experiments}")
        print(f"Most common experiment compound: {row.max_freq_compound}")
        print("---")

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
        query_table()

