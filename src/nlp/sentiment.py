import os
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import NGram, VectorAssembler, CountVectorizer


class SentimentAnalyser(object):
    """
    Sentiment analysis by Inverse document frequency of N-Grams from tweets,
    classified using a Logistic Regression classifier.
    """
    def __init__(self):
        self._pipeline = None
        self._classifier = None
        self._report = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")
        self.positive = 0.0
        self.negative = 1.0

    def load(self, pipeline_path, classifier_path):
        """
        Load trained pipelines from disk
        Args:
            pipeline_path: folder with transformer pipeline data
            classifier_path: folder with classifier data

        Returns:
            sets self up with models
        """
        if os.path.exists(pipeline_path):
            self._pipeline = PipelineModel.load(pipeline_path)
        if os.path.exists(classifier_path):
            self._classifier = LogisticRegressionModel.load(classifier_path)

    def train(self, train_df):
        """
        If pipeline and classifier are not set, create new and train with input training
        set of tweets
        Args:
            train_df: dataframe of tweets

        Returns:
            predicted labels on train set after training
        """
        if self._pipeline is None:
            pipeline = SentimentAnalyser._ngram_model()
            pipeline_trained = pipeline.fit(train_df)
            self._pipeline = pipeline_trained

        train_fit = self._pipeline.transform(train_df)

        if self.negative is None:
            self.negative = train_fit.filter(train_fit.target == 0).first().label
        if self.positive is None:
            self.positive = train_fit.filter(train_fit.target == 1).first().label

        if self._classifier is None:
            lr = SentimentAnalyser._lr_classifier()
            lr_fit = lr.fit(train_fit)
            self._classifier = lr_fit

        train_predictions = self._classifier.transform(train_fit)

        return train_predictions

    def predict(self, test_df):
        """
        Assuming model has been trained, predict labels for a set of tweets
        Args:
            test_df: dataframe of tweets

        Returns:
            dataframe with predicted labels
        """
        if self._classifier is None or self._pipeline is None:
            raise AssertionError("Need to train first")

        test_transformed = self._pipeline.transform(test_df)
        predictions = self._classifier.transform(test_transformed)

        return predictions

    def classification_report(self, predictions):
        """
        Print out the ROC AUC and accuracy for a set of predictions
        Args:
            predictions: dataframe of predictions

        Returns:
            prints roc auc and accuracy
        """
        bin_rep = self._report.evaluate(predictions)
        accuracy = predictions.filter(predictions.label == predictions.prediction).count() / float(predictions.count())

        print("RocAuc: %.4f, Accuracy: %.4f" % (bin_rep, accuracy))

    def count_sentiments(self, predictions):
        """
        Count the positive and negative sentiment tweets in a set
        Args:
            predictions: dataframe of tweet predictions

        Returns:
            [positives, negatives]
        """
        positive = predictions.filter(predictions.prediction == self.positive)
        negative = predictions.filter(predictions.prediction == self.negative)

        print("Tweets positive: {:,}, negative: {:,}".format(positive.count(), negative.count()))
        return positive, negative

    @staticmethod
    def _lr_classifier(max_iter=100, reg_param=0.1):
        return LogisticRegression(maxIter=max_iter, regParam=reg_param)

    @staticmethod
    def _ngram_model(input_col=("text", "target"), n=3):
        tokeniser = [Tokenizer(inputCol=input_col[0], outputCol="words")]
        ngrams = [
            NGram(n=i, inputCol="words", outputCol="%d_grams" % i) for i in range(1, n + 1)
        ]
        count_vectoriser = [
            CountVectorizer(vocabSize=5000, inputCol="%d_grams" % i, outputCol="%d_tf" % i) for i in range(1, n + 1)
        ]
        inverse_doc_freq = [
            IDF(inputCol="%d_tf" % i, outputCol="%d_idf" % i, minDocFreq=5) for i in range(1, n + 1)
        ]

        vector_assembler = [
            VectorAssembler(inputCols=["%d_idf" % i for i in range(1, n + 1)], outputCol="features")
        ]

        string_index = [
            StringIndexer(inputCol=input_col[1], outputCol="label", handleInvalid="keep")
        ]

        return Pipeline(
            stages=tokeniser + ngrams + count_vectoriser + inverse_doc_freq + vector_assembler + string_index)

    @staticmethod
    def simple_tfidf_model(input_col=("text", "target")):
        tokeniser = Tokenizer(inputCol=input_col[0], outputCol="words")
        hashtf = HashingTF(numFeatures=2 ** 16, inputCol="words", outputCol="tf")
        idf = IDF(inputCol="tf", outputCol="features", minDocFreq=5)

        string_index = StringIndexer(inputCol=input_col[1], outputCol="label", handleInvalid="keep")
        pipeline = Pipeline(stages=[tokeniser, hashtf, idf, string_index])

        return pipeline

