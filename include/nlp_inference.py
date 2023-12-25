import pandas as pd
import duckdb
from transformers import pipeline
from include.global_variables import global_variables as gv
import spacy
from googleapiclient.discovery import build
from textblob import TextBlob
from span_marker import SpanMarkerModel


duck_db_instance_name = (
    "dwh"  # when changing this value also change the db name in .env
)

def get_comments_data(db=f"/usr/local/airflow/{duck_db_instance_name}"):

    cursor = duckdb.connect(db)

    # get global surface temperature data
    comments_data = cursor.execute(
        f"""SELECT * 
        FROM {gv.COMMENTS_IN_TABLE_NAME};"""
    ).fetchall()

    comments_col_names = cursor.execute(
        f"""SELECT column_name from information_schema.columns where table_name = '{gv.COMMENTS_IN_TABLE_NAME}';"""
    ).fetchall()

    cursor.close()

    df = pd.DataFrame(
        comments_data, columns=[x[0] for x in comments_col_names]
    )

    return df



pretrained_ner = "/usr/local/airflow/models/span-marker-mbert-base-multinerd"
ner_model = SpanMarkerModel.from_pretrained(pretrained_ner)

# Function to extract 'PER' entities from the 'entities' column
def extract_per_entities(entities):
    return [entity['span'] for entity in entities if entity['label'] == 'PER']

def ner_and_extract_per(text, model):
    try:
        entities = model.predict(text)
        per_entities = extract_per_entities(entities)
        return per_entities
    except Exception as e:
        print(f"Exception in NER processing: {e}")
        return []


pretrained_sentiment = "/usr/local/airflow/models/bart-large-mnli"
sentiment_model = pipeline(
    "zero-shot-classification",
    model=pretrained_sentiment,
    return_all_scores=True
)

def zero_shot_sentiment_analysis(row, model):
  if not row['entities']:
    return {}
  candidate_labels = ["positive", "negative", "neutral"]
  result = {}
  for entity in row['entities'] :
    hypothesis_template = f"the sentiment towards {entity} is {{}}."
    output = model([row['comment']], candidate_labels, hypothesis_template=hypothesis_template)

    # Extract the first element of the list (assuming it contains the most relevant results)
    top_result = output[0]

    # Convert the list of dictionaries to a single dictionary
    label_score = {label: score for label, score in zip(top_result['labels'], top_result['scores'])}

    result[entity] = max(label_score, key=label_score.get)

  return result



def main():
    df_comments = get_comments_data(db=f"/usr/local/airflow/{duck_db_instance_name}")
    comments = df_comments['TEXT'].tolist()

    # Assuming comments is a list. Convert to DataFrame
    df = pd.DataFrame(comments[:9000], columns=['comment'])
    # print(df)
    # Get the number of rows in the DataFrame
    num_rows = len(df)
    print(f"number of rows : {num_rows}")

    df['entities'] = df['comment'].apply(lambda x: ner_and_extract_per(x, ner_model))
    df['ner_sentiment_analysis'] = df.apply(lambda row: zero_shot_sentiment_analysis(row, sentiment_model), axis=1)
    df = df[df['ner_sentiment_analysis'].notna() & (df['ner_sentiment_analysis'] != {})]
    flattened_data = pd.DataFrame([(entity, sentiment) for entities in df['ner_sentiment_analysis'] for entity, sentiment in entities.items()], columns=['name', 'sentiment'])
    sentiment_counts = flattened_data.groupby(['name', 'sentiment']).size().reset_index(name='count')
    
    # Display the grouped data
    print(sentiment_counts)

    # connect to an in-memory database
    con = duckdb.connect(f"/usr/local/airflow/{duck_db_instance_name}")

    # create the table "my_table" from the DataFrame "my_df"
    con.execute(f"CREATE OR REPLACE TABLE {'output' + '_' + gv.MY_VIDEO_ID} AS SELECT * FROM sentiment_counts")
    # Get the sum of column 'A'
    sum_count = sentiment_counts['count'].sum()
    print(f"sum : {sum_count}")  # Output: 6
    print(sentiment_counts)


if __name__ == "__main__":
    main()
