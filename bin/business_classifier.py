from pyspark.sql.types import StructType, StructField, StringType, LongType
from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector
from langdetect import detect
from profanity_check import predict
import pickle

from sparkcc import CCSparkJob


class BusinessClassifier(CCSparkJob):

    name = "BusinessClassifier"

    def __init__(self):
        CCSparkJob.__init__(self)
        self.output_schema = StructType([
            StructField("topic", StringType(), True),
            StructField("count", LongType(), True)
        ])
        # load the model
        with open('businessclassifier.pkl', 'rb') as f:
            self.clf, self.vectorizer, self.target_names = pickle.load(f)

    # predict
    def predict(self, document):
        x_test = self.vectorizer.transform([document])
        pred = self.clf.predict(x_test)
        return self.target_names[pred[0]]

    def get_text(self, record):
        try:
            content = record.content_stream().read()
            encoding = EncodingDetector.find_declared_encoding(content,
                                                               is_html=True)
            soup = BeautifulSoup(content, "lxml", from_encoding=encoding)
            # strip all script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            return soup.get_text(" ", strip=True)
        except:
            return ""

    def process_record(self, record):
        # WET/plain text record
        if self.is_wet_text_record(record):
            text = record.content_stream().read().decode('utf-8')
        # html record
        elif self.is_html(record):
            text = self.get_text(record)
        else:
            yield 'error.unknown.record.type', 1
            return

        if len(text) < 500:
            yield 'tiny.page', 1
            return

        try:
            lang = detect(text)
            if lang != 'en':
                # skip non-English pages
                yield 'non.english.page', 1
                return
        except:
            yield 'error.lang.detect', 1
            return

        if predict([text])[0]:
            yield 'adult', 1
            return

        topic = self.predict(text)
        yield topic, 1

if __name__ == '__main__':
    job = BusinessClassifier()
    job.run()
