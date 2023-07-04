import gc
import os
from datetime import datetime

import findspark
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel
from pyspark.ml.classification import MultilayerPerceptronClassificationModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col, lower, trim, regexp_replace
from pyspark.sql.types import *
from sparknlp import EmbeddingsFinisher
from sparknlp.annotator import BertSentenceEmbeddings
from sparknlp.annotator import Normalizer
from sparknlp.annotator import SentenceEmbeddings
from sparknlp.annotator import (Tokenizer, LemmatizerModel, StopWordsCleaner)
from sparknlp.base import Finisher, DocumentAssembler

# pretrained model path
model_path = 'models/mlp_BERT_sentence'

findspark.init()

# setting env variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

os.environ['HADOOP_HOME'] = "path/to/hadoop"
os.environ['SPARK_HOME'] = "path/to/spark"

spark = SparkSession.builder \
    .appName("Spark NLP") \
    .master("local[4]") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.executor.memoryOverhead", "16g") \
    .config("spark.driver.memory", "32G") \
    .config("spark.python.worker.memory", "32G") \
    .config("spark.sql.analyzer.maxIterations", "6000") \
    .config("spark.driver.cores", "10") \
    .config("spark.driver.maxResultSize", "0") \
    .config("spark.kryoserializer.buffer.max", "2000M") \
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:4.4.4") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.executor.heartbeatInterval", "5s") \
    .config("spark.executor.memory", "20g") \
    .config("spark.sql.codegen.wholeStage", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

# download required feature models

# bert_base_uncased -> the model is imported from https://tfhub.dev/google/bert_uncased_L-12_H-768_A-12/1
# small_bert_L2_768 -> the model is imported from https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-2_H-768_A-12/1
bert_embedding_token = BertEmbeddings.pretrained('bert_base_uncased', 'en')

# sent_bert_base_uncased -> the model is imported from https://tfhub.dev/google/bert_uncased_L-12_H-768_A-12/1
bertSentenceEmbenddingsPretrained = BertSentenceEmbeddings.pretrained("sent_bert_base_uncased", "en")

# download required feature models
lemmatizer = LemmatizerModel.pretrained()


def clean_text_bert_sentence(df, column_name="comment", new_column="comment"):
    print("Start clean text ", datetime.now())

    # Text preprocessing pipeline

    # 1. Text cleaning

    # 1.a Case normalization
    lower_case_df = df.withColumn(new_column, lower(col(column_name)))

    # 1.b Trimming
    trimmed_df = lower_case_df.select(trim(col(new_column)).alias(new_column), "id")

    # 1.c Filter out punctuation symbols
    no_punct_df = trimmed_df.select((regexp_replace(col(new_column), "[^\w\d\s]", "")).alias(new_column), "id")

    # 1.d Filter out any internal extra whitespace
    no_extra_wspace_df = no_punct_df.select(trim(regexp_replace(col(new_column), " +", " ")).alias(new_column), "id")

    del df
    del lower_case_df
    del trimmed_df
    del no_punct_df

    gc.collect()
    print("End clean text ", datetime.now())

    return no_extra_wspace_df


def clean_text(df, column_name="comment"):
    # Text preprocessing pipeline
    print("***** Text Preprocessing Pipeline *****\n")

    # convert the text into a Spark-NLP annotator-ready form
    documentAssembler = DocumentAssembler().setInputCol(column_name) \
        .setOutputCol('document')

    tokenizer = Tokenizer() \
        .setInputCols(['document']) \
        .setOutputCol('token')

    normalizer = Normalizer() \
        .setInputCols(["token"]) \
        .setOutputCol("normalized") \
        .setLowercase(True) \
        .setCleanupPatterns(["""[^\w\d\s]"""])  # remove punctuations (keep alphanumeric chars)

    # note that lemmatizer needs a dictionary. So we used the pre-trained model (note that it defaults to english)
    lemmatizer = LemmatizerModel.pretrained("lemma_antbnc") \
        .setInputCols(['normalized']) \
        .setOutputCol('lemma')

    stopwords_cleaner = StopWordsCleaner.pretrained("stopwords_en") \
        .setInputCols(['lemma']) \
        .setOutputCol('clean_lemma') \
        .setCaseSensitive(False)

    # convert tokens back to human-readable form
    finisher = Finisher() \
        .setInputCols(['clean_lemma']) \
        .setOutputCols("finisher_result") \
        .setCleanAnnotations(False) \
        .setOutputAsArray(True)

    pipeline = Pipeline() \
        .setStages([
        documentAssembler,
        tokenizer,
        normalizer,
        lemmatizer,
        stopwords_cleaner,
        finisher
    ])

    cleaned_df = pipeline.fit(df).transform(df).select(
        ['comment', 'document', 'clean_lemma', 'finisher_result', 'id'])

    return cleaned_df


def bert_embedding(cleaned_df):
    df_bert = cleaned_df.select('document', 'clean_lemma', 'id', 'comment')

    cleaned_df.unpersist()
    del cleaned_df

    embeddings = bert_embedding_token.setInputCols("document", "clean_lemma") \
        .setOutputCol("embeddings")

    # the result of BERT are multiple vectors, we will collapse them into one using the average pooling strategy
    embeddingsSentence = SentenceEmbeddings() \
        .setInputCols(["document", "embeddings"]) \
        .setOutputCol("sentence_embeddings") \
        .setPoolingStrategy("AVERAGE")

    finisher = EmbeddingsFinisher() \
        .setInputCols("sentence_embeddings") \
        .setOutputCols("embeddings_result")

    bert_pipeline = Pipeline(stages=[embeddings, embeddingsSentence, finisher])

    df_bert = bert_pipeline.fit(df_bert).transform(df_bert) \
        .select(col("comment"), col('id'), explode("embeddings_result").alias('features')) \
        .select(['comment'] + ['id'] + [expr('features[' + str(x) + ']') for x in range(768)])

    # VectorAssembler
    vector_assembler = VectorAssembler(inputCols=df_bert.columns[2:], outputCol='features').setHandleInvalid("skip")

    features_assembled = vector_assembler.transform(df_bert).select('comment', 'features', 'id')

    df_bert.unpersist()
    del df_bert

    return features_assembled


def bert_sentence_embedding(df):
    # convert the text into a Spark-NLP annotator-ready form
    documentAssembler = DocumentAssembler() \
        .setInputCol('comment') \
        .setOutputCol('document')

    embeddings = bertSentenceEmbenddingsPretrained.setInputCols("document") \
        .setOutputCol("embeddings")

    finisher = EmbeddingsFinisher() \
        .setInputCols("embeddings") \
        .setOutputCols("embeddings_result")

    bert_pipeline = Pipeline(stages=[documentAssembler, embeddings, finisher])

    df_bert = bert_pipeline.fit(df).transform(df) \
        .select(col('comment'), col('id'), explode("embeddings_result").alias('features')) \
        .select(['comment'] + ['id'] + [expr('features[' + str(x) + ']') for x in range(768)])

    # VectorAssembler
    vector_assembler = VectorAssembler(inputCols=df_bert.columns[2:], outputCol='features').setHandleInvalid("skip")

    features_assembled = vector_assembler.transform(df_bert).select('comment', 'features', 'id')

    df_bert.unpersist()
    del df_bert

    return features_assembled


def word2vec_embedding(cleaned_df):
    word2vec_pipeline_fitted = PipelineModel.load("Word2Vec_pipeline")
    df_word2vec = cleaned_df.select('document', 'clean_lemma', 'id', 'comment')

    cleaned_df.unpersist()
    del cleaned_df

    df_word2vec = word2vec_pipeline_fitted.transform(df_word2vec) \
        .select(col("comment"), col('id'), explode("embeddings_result").alias('features')) \
        .withColumn("features", vector_to_array("features")) \
        .select(['comment'] + ['id'] + [expr('features[' + str(x) + ']') for x in range(300)])

    # cast each column to float
    for col_name in df_word2vec.columns:
        if col_name == "comment" or col_name == "id":
            continue
        df_word2vec = df_word2vec.withColumn(col_name, col(col_name).cast(FloatType()))

    # VectorAssembler
    vector_assembler = VectorAssembler(inputCols=df_word2vec.columns[2:], outputCol='features').setHandleInvalid("skip")

    features_assembled = vector_assembler.transform(df_word2vec).select('comment', 'features', 'id')

    df_word2vec.unpersist()
    del df_word2vec

    return features_assembled


def tfidf_pca_features(cleaned_df):
    features_pca_fitted = PipelineModel.load("PCA_pipeline")

    features_pca = features_pca_fitted.transform(cleaned_df) \
        .select(col('id'), col('comment'), col("pca_features").alias("features")) \
        .withColumn("features", vector_to_array("features")) \
        .select(['id'] + ['comment'] + [expr('features[' + str(x) + ']') for x in range(500)])

    # cast each column to float
    for col_name in features_pca.columns:
        if col_name == "comment" or col_name == "id":
            continue
        features_pca = features_pca.withColumn(col_name, col(col_name).cast(FloatType()))

    # VectorAssembler
    vector_assembler = VectorAssembler(inputCols=features_pca.columns[:2], outputCol='features') \
        .setHandleInvalid("skip")

    features_pca = vector_assembler.transform(features_pca).select('features', 'id', 'comment')
    gc.collect()

    return features_pca


def data_preprocessing(comment_text):
    if not type(comment_text) == list:
        comment_text = [comment_text]

    print("Start data processing ", datetime.now())

    df = pd.DataFrame(comment_text)
    df_reddit = spark.createDataFrame(df)

    # cleaned_df = clean_text(df_reddit)
    cleaned_df = clean_text_bert_sentence(df_reddit)

    print("End data processing ", datetime.now())

    return cleaned_df


def features_extraction(df, feature_engineering):
    if feature_engineering == 'BERT':
        return bert_embedding(df)
    if feature_engineering == 'tfidf':
        return tfidf_pca_features(df)
    if feature_engineering == 'Word2Vec':
        return word2vec_embedding(df)
    if feature_engineering == 'BERT_sentence':
        return bert_sentence_embedding(df)


def get_prediction(comment_text):
    data = data_preprocessing(comment_text)

    features = features_extraction(data, 'BERT_sentence')

    del data

    model = MultilayerPerceptronClassificationModel.load(model_path)

    print("Start model trasformation ", datetime.now())
    text_prediction = model.transform(features).select('prediction', 'id').toPandas().to_dict(orient='records')
    print("End model trasformation ", datetime.now())

    del model
    del features
    gc.collect()

    return text_prediction


app = Flask(__name__)
CORS(app)


@app.route('/prediction', methods=['POST'])
def prediction():
    if not request.is_json:
        return "Bad request", 400

    data = request.get_json()

    if 'textComments' in data:
        json_prediction = get_prediction(data['textComments'])
    else:
        return "Please specify textComments in the request", 400

    response = jsonify(json_prediction)
    response.headers.add('Access-Control-Allow-Origin', '*')

    return response


if __name__ == '__main__':
    app.run(port=8080)
