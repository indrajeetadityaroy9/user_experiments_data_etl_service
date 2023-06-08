from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import os
import pandas as pd

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URL', 'postgres://postgres:password@db:5432/postgres')
db = SQLAlchemy(app)

class UserExperimentStats(db.Model):
    __tablename__ = 'user_experiment_stats'
    user_id = db.Column(db.Integer, primary_key=True)
    total_experiments = db.Column(db.Integer)
    avg_experiments = db.Column(db.Float)
    max_freq_compound = db.Column(db.String)

def etl():
    # Load CSV files
    user_experiments = pd.read_csv('data/user_experiments.csv')
    users = pd.read_csv('data/users.csv')
    compounds = pd.read_csv('data/compounds.csv', sep='\t')

    #Process files to derive features

    #Removes whitespace from df columns
    users.columns = users.columns.str.strip()
    #Removes whitespace from df columns
    user_experiments.columns = user_experiments.columns.str.strip()
    #Removes whitespace from df columns
    compounds.columns = compounds.columns.str.strip()
    #Cleans and converts df columns
    compounds.columns = compounds.columns.str.replace(',', '')
    compounds['compound_id'] = compounds['compound_id'].str.replace(',', '').astype(int)

    #Count of each val in df `user_id` column
    user_experiment_counts = user_experiments['user_id'].value_counts()
    #Mean of experiments count
    average_experiments_per_user = user_experiment_counts.mean()

    #Replace `experiment_compound_ids` df column with list of ids 
    #Converts each compound id from list into seperate rows
    user_experiments = user_experiments.assign(experiment_compound_ids=user_experiments['experiment_compound_ids'].str.split(';')).explode('experiment_compound_ids')
    #Cleans and converts df columns
    user_experiments['experiment_compound_ids'] = user_experiments['experiment_compound_ids'].str.replace(',', '').astype(int)

    #Joins `user_experiments` df with `compounds` df on `experiment_compound_ids`
    merged_table = user_experiments.merge(compounds, left_on='experiment_compound_ids', right_on='compound_id', how='left')
    #Cleans and converts df columns
    merged_table['compound_name'] = merged_table['compound_name'].str.rstrip(',')
    
    #Count of each val in df column
    #Group rows by `user_id`
    user_compound_counts = merged_table.groupby('user_id')['compound_name'].value_counts()
    #Compound with max frequency/count
    user_high_freq_compounds = user_compound_counts.groupby('user_id').idxmax()

    # Upload processed data into a database

    # DB connection session
    session = db.session
    
    # Loop through each user
    for user_id in user_experiment_counts.index:
        total_experiments = int(user_experiment_counts.loc[user_id].item())
        avg_experiments = float(average_experiments_per_user)
        most_common_compound = str(user_high_freq_compounds.loc[user_id][1])
        
        # Create a new DB entry object
        user_stats = UserExperimentStats(
            user_id=user_id,
            total_experiments=total_experiments,
            avg_experiments=avg_experiments,
            max_freq_compound=most_common_compound
            )
        
        # Add entry object to DB session
        session.add(user_stats)
    
    # Commit session changes to DB
    session.commit()


# Your API that can be called to trigger your ETL process
@app.route('/trigger', methods=['POST'])
def trigger_etl():
    db.create_all()
    # Trigger your ETL process here
    etl()
    return {"message": "ETL process started"}, 200

if __name__ == '__main__': 
    app.run(host='0.0.0.0', port=5000)
