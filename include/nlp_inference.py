import pandas as pd
import duckdb
from transformers import pipeline
from transformers import BertForTokenClassification, BertTokenizer, TokenClassificationPipeline
from include.global_variables import global_variables as gv

pretrained_sentiment = "/usr/local/airflow/models/bertweet-base-sentiment-analysis"
pretrained_ner = "/usr/local/airflow/models/bert-base-NER"
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

class ModelLoader:
    _sentiment_model = None
    _ner_model = None

    @classmethod
    def get_sentiment_model(cls):
        if cls._sentiment_model is None:
            cls._sentiment_model = pipeline(
                "sentiment-analysis",
                model=pretrained_sentiment,
                tokenizer=pretrained_sentiment,
                top_k=None  # replace return_all_scores=True with this
            )
        return cls._sentiment_model

    @classmethod
    def get_ner_model(cls):
        if cls._ner_model is None:
            model = BertForTokenClassification.from_pretrained(pretrained_ner)
            tokenizer = BertTokenizer.from_pretrained(pretrained_ner)
            cls._ner_model = TokenClassificationPipeline(
                model=model,
                tokenizer=tokenizer,
                aggregation_strategy="simple"  # replace grouped_entities=True with this
            )
        return cls._ner_model



def sentiment_analysis(text):
    model = ModelLoader.get_sentiment_model()
    output = model(text)
    result = {elm["label"]: elm["score"] for elm in output[0]}
    return max(result, key=result.get)

def ner(text):
    model = ModelLoader.get_ner_model()
    entities = model(text)
    # Convert any numpy objects to native Python types here
    return [{k: (v.tolist() if hasattr(v, 'tolist') else v) for k, v in entity.items()} for entity in entities]


# Assuming you have the function definitions for sentiment_analysis and ner
# from your original code

def process_data(row):
    try:
        text = row['text'][:128]  # Truncate text
        return pd.Series([text, sentiment_analysis(text), ner(text)])
    except Exception as e:
        # handle or log exception
        return pd.Series([None, None, None])
        print(f"exception:{e}")
    
import spacy
from googleapiclient.discovery import build
from textblob import TextBlob


def main():
    print('testing model')
    print(sentiment_analysis('Jacob, Youssef, sad'))
    print(ner('Jacob, Youssef, sad'))
    # comments = get_comments(VIDEO_ID)
    df = get_comments_data(db=f"/usr/local/airflow/{duck_db_instance_name}")
    comments = df['TEXT'].tolist()

    # Assuming comments is a list. Convert to DataFrame
    df = pd.DataFrame(comments[:9000], columns=['text'])
    # print(df)
    # Get the number of rows in the DataFrame
    num_rows = len(df)
    print(f"number of rows : {num_rows}")
    # Process data
    processed_df = df.apply(process_data, axis=1)
    processed_df.columns = ["text", "sentiment", "entities"]

    exploded_df = processed_df.explode('entities').rename(columns={'entities': 'entity'})
    # print(exploded_df)
    # Filter where entity group is "PER" and get the corresponding names
    names_df = exploded_df[exploded_df['entity'].apply(lambda x: x['entity_group'] if isinstance(x, dict) and 'entity_group' in x else None) == 'PER']
    names_df = names_df.copy()
    names_df.loc[:, 'name'] = names_df['entity'].apply(lambda x: x['word'] if isinstance(x, dict) and 'word' in x else None)

    # Drop the entity column as it's no longer needed
    names_df = names_df.drop('entity', axis=1)

    # Group by text and sentiment to get list of names for each text
    result_df = names_df.groupby(['text', 'sentiment'])['name'].apply(list).reset_index()

    # Explode the names column along with sentiment
    exploded_df = result_df.explode('name')

    # Remove non-alphabetical characters and convert to lowercase
    exploded_df['name'] = exploded_df['name'].str.replace("[^a-zA-Z]", "").str.lower()

    # Group by 'name' and 'sentiment' and count the occurrences
    sentiment_counts = exploded_df.groupby(['name', 'sentiment']).size().reset_index(name='count')

    # connect to an in-memory database
    con = duckdb.connect(f"/usr/local/airflow/{duck_db_instance_name}")

    # create the table "my_table" from the DataFrame "my_df"
    con.execute("CREATE OR REPLACE TABLE output AS SELECT * FROM sentiment_counts")
    # Get the sum of column 'A'
    sum_count = sentiment_counts['count'].sum()
    print(f"sum : {sum_count}")  # Output: 6
    print(sentiment_counts)


if __name__ == "__main__":
    main()