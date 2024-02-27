# Sarcasm detection on Reddit with PySpark

Final project for the **Big Data Computing** course held by prof. Gabriele Tolomei at **La Sapienza University of Rome** in A.Y. 2022/2023.
In this project we addressed a binary classification problem aimed at detecting sarcasm in Reddit comments with _Machine Learning_ and _Big Data_ tools.

We chose 3 feature engineering methods and 4 classification models and compared them to find out which combination performs better:
- Feature engineering: _TF-IDF_, _Word2Vec_, and _BERT_.
- Classification models: _Logistic regression_, _Random forest_, _Linear SVC_, and _Multilayer perceptron_.

For further details you can refer to the [presentation slides](Sarcasm detection on Reddit.pdf)

## Datasets

We evaluate our approach on two public datasets available on **Kaggle**:
- [Sarcasm on Reddit](https://www.kaggle.com/datasets/danofer/sarcasm): 1.3M labelled comments.
- [Datasets for NLP - sarcasm](https://www.kaggle.com/datasets/toygarr/datasets-for-natural-language-processing): 29k labelled comments.

## Training

To train and evaluate each classification model run the Jupyter Notebooks in this order:
- First dataset:
1. [Sarcasm_detection_on_Reddit_1M_dataset_token.ipynb](1_Sarcasm_detection_on_Reddit_1M_dataset_token.ipynb)
2. [Sarcasm_detection_on_Reddit_1M_dataset_sentence.ipynb](2_Sarcasm_detection_on_Reddit_1M_dataset_sentence.ipynb)
- Second dataset:
3. [Sarcasm_detection_on_Reddit_29k_dataset_token.ipynb](3_Sarcasm_detection_on_Reddit_29k_dataset_token.ipynb)
4. [Sarcasm_detection_on_Reddit_29k_dataset_sentence.ipynb](4_Sarcasm_detection_on_Reddit_29k_dataset_sentence.ipynb)

## Demo
We propose a [chrome extension](demo.py) to perform sarcasm detection on Reddit comment in real-time using our best classification model.
Notice that to run it you need the model checkpoint saved locally in the following paths:

    ├── ... 
    ├── models                    
        └── model_name      # Saved model checkpoint
	        └── ...

## Group members

- [Chiara Ballanti](https://github.com/Ballants)
- [Paolo Pio Bevilacqua](https://github.com/ppbevilacqua)
