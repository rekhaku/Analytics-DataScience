import csv
import urllib
from gc import collect

import seaborn as sns
import pandas as pd
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from parse import parse
from wordcloud import WordCloud
import re

import glob
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from matplotlib import pyplot as plt

# methods:

"""
def parse_log_by_slab(fmt):
    with open("log_parsed.csv", "w", newline="", encoding="utf-8") as csvfile:
        # Create a CSV writer object
        log_temp_file = csv.writer(csvfile)

        # Write the header row
        log_temp_file.writerow(["Host", "Time", "Request", "Protocol", "Status", "Size", "Host_prefix", "Browser"])
        for item in sample_logs:
            res = parse(fmt, item)
            #date_time_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'

           # access_time = re.search(date_time_pattern, res['time']).group(1)
            log_temp_file.writerow(
                [res['host'], res['time'], res['request'], res['protocol'], res['status'], res['size'],
                 res['host_prefix'],
                 res['browser']])



"""

def pre_process_request(document):
    endpoints = []
    protocols = []
    images = []
    products = []
    filter = []
    corpus = []
    pdts = " "
    for token in df["Request"]:
        if "image" in token:
            images.append(token)
        elif "filter" in token:
            filter.append(urllib.parse.unquote(token, encoding='utf-8'))
        elif "product" in token:
            pdt_pattern = r'/([^/]+)/?$'
            pdt = urllib.parse.unquote(re.search(pdt_pattern, token).group(1), encoding='utf-8')
            print(pdt)
            pdts = pdts + " " + pdt
            products.append(pdt)

        else:
            endpoints.append(urllib.parse.unquote(token, encoding='utf-8'))

    corpus = [pdts]
    protocols.append(token)
    document.append(images)
    document.append(filter)
    document.append(products)
    document.append(endpoints)
    document.append(protocols)
    document.append(corpus)

def pre_process_data():
    host_time_request = df.loc[:, ['Host', 'Time','Request','Status']]
    return host_time_request
def plot_counts(count_vector, count_matrix, x_pattern, title, x_label):
    plt.figure(figsize=(10, 10))
    chy = count_vector.get_feature_names_out()
    chx = count_matrix.toarray().sum(axis=0)
    sum_wrd = count_matrix.sum(axis=0)
    x_value, y_value = top_n_values(chy, chx, 10)
    plt.title(title)
    plt.ylabel(x_label)
    plt.xticks(y_value)
    plt.xticks(rotation='vertical')
    plt.xlabel('Count')
    df12 = pd.DataFrame(y_value, x_value).to_csv('temp1' + x_label + '.csv')
    print(df12)
    plt.barh(data=count_matrix, width=y_value, y=x_value, height=0.5, color="#008080")
    plt.show()
    word_freq = [(word, sum_wrd[0, idx]) for word, idx in count_vector.vocabulary_.items()]
    word_freq = sorted(word_freq, key=lambda x: x[1], reverse=True)
    print(word_freq)

    wordcloud = WordCloud(width=100, height=100, background_color="white", relative_scaling=0.5, mode="RGB").generate_from_frequencies(dict(word_freq))
    plt.figure(figsize=(10,10))
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.title(title)
    plt.show()



def plot_tdf(tf_vector, tf_vectorizer, pattern, title, y_label):
    thy = tf_vectorizer.get_feature_names_out()
    thx = tf_vector.toarray().sum(axis=0)
    sum_wrd = tf_vector.sum(axis=0)
    x1_value, y1_value = top_n_values(thx, thy, 10)
    df11 = pd.DataFrame(x1_value, y1_value).to_csv('temp1'+y_label+'.csv')
    print(df11)
    # Add a newline character to separate each element

    plt.barh(data=tf_vectorizer, width=x1_value, y=y1_value, height=0.5, color="#FF5555")
    plt.xlabel(y_label)
    plt.xticks(x1_value)
    plt.xticks(rotation='vertical')
    plt.xlabel('Usage')
    plt.title(title)
    plt.show()

    word_freq = [(word, sum_wrd[0, idx]) for word, idx in tf_vectorizer.vocabulary_.items()]
    word_freq = sorted(word_freq, key=lambda x: x[1], reverse=True)
    print(word_freq)

    wordcloud = WordCloud(width=400, height=400, background_color="white", colormap="viridis",  mode="RGB").generate_from_frequencies(dict(word_freq))
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.title(title)
    plt.show()


def top_n_values(x_param, y_param, n):
    freq_dict = dict(zip(x_param, y_param))
    print(freq_dict)
    sorted_word_freq = sorted(freq_dict.items(), key=lambda x: x[1], reverse=True)
    # Get the top 10 words with the highest counts
    top_n_words = sorted_word_freq[:n]
    # Extract words and counts separately
    top_n_words, top_n_counts = zip(*top_n_words)
    print(top_n_words, top_n_counts)
    return top_n_words, top_n_counts

"""
sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)

raw_data_files = glob.glob("log/access_1.log")
raw_data_files

base_df = spark.read.text(raw_data_files)
base_df.printSchema()
base_df_rdd = base_df.rdd

print((base_df.count(), len(base_df.columns)))

sample_logs = [item['value'] for item in base_df_rdd.take(2000)]

with open("temp_file", "w") as file:
    # Iterate over the list and write each element to the file
    for item in sample_logs:
        file.write(item + "\n")  # Add a newline character to separate each element

"""
fmt = '{host} - - [{time}] "{method} {request} {protocol}" {status} {size} {host_prefix} {browser}'
#parse_log_by_slab(fmt)

df = pd.read_csv("log_final_1.csv")

document = []
pre_process_request(document)

print("corpus is", document[5])

# plt.figure(figsize=(10, 10))

"""
host_pattern = r'(\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3})'
cnt_vectorizer_host = CountVectorizer(token_pattern=host_pattern)
X = cnt_vectorizer_host.fit_transform(df["Host"])
plot_counts(cnt_vectorizer_host, X, host_pattern, "Frequent Hosts Statistics", "Host")

request_pattern = r'[^\s]+'
cnt_vectorizer_req = CountVectorizer(token_pattern=request_pattern)
x_req = cnt_vectorizer_req.fit_transform(document[2])
plot_counts(cnt_vectorizer_req, x_req, request_pattern, "Product Browse Statistics", "Products")



tfidf_vectorizer_pdt = TfidfVectorizer(token_pattern=request_pattern)
tfidf_vector_pdt = tfidf_vectorizer_pdt.fit_transform(document[5])

plot_tdf(tfidf_vector_pdt, tfidf_vectorizer_pdt, request_pattern, "Popular Product Statistics", "Products")

host_request_df1 = pre_process_data()

daily_host_hit_count_df = (host_request_df1
                           .groupby('Host')
                           .size()
                           .reset_index(name="Count")
                           )
sorted_df = daily_host_hit_count_df.sort_values(by='Count', ascending=False).head(10)
df13 = pd.DataFrame(sorted_df).to_csv("test1.csv")
plt.plot(sorted_df['Host'], sorted_df['Count'],'-gD',color="#008080" )
plt.xticks(rotation='vertical')
plt.yticks(sorted_df['Count'])
plt.title("Daily Host Hit Statistics")
plt.xlabel("Host")
plt.ylabel("Count")
plt.show()

"""
host_request_df1 = pre_process_data()
access_pattern_df = host_request_df1.sort_values(by='Host',ascending=False)
grouped_values = access_pattern_df.sort_values(by='Time',ascending=True).groupby(['Host'])['Request'].apply(list).reset_index()
df12 = pd.DataFrame(grouped_values).to_csv("Usage_pattern1.csv")
#print(grouped_values['Request'])
#print(grouped_values[5])
plt.title("User access pattern")
#plt.plot(grouped_values[0],color="#FF00CC")
#plt.plot(grouped_values[2],color="#9900DD")
plt.plot(grouped_values['Request'][3], '-*',label=grouped_values['Host'][3])
plt.plot(grouped_values['Request'][174], '-*',label=grouped_values['Host'][174])
plt.plot(grouped_values['Request'][166], '-*',label=grouped_values['Host'][166])
plt.yticks( rotation=45)
plt.legend()

plt.show()

"""
status_code_df = (host_request_df1.groupby('Status').size().reset_index(name="Count"))
plt.bar(x= status_code_df['Status'], height=status_code_df['Count'], color="#008080")
plt.xlabel("Count")
plt.ylabel("Status")
plt.title("Website Hit Statistics")
df12 = pd.DataFrame(status_code_df).to_csv("hsthit.csv")
plt.show()

"""

df3 = {"Host":['109.125.150.157'],"Request":[ '/login/entryPage=HEADER_ACCOUNT&sourceContext=DEFAULT' ,'/image/11926?name=w-vesta-cordless-ironbox-1380W&wh=200x200', '/product/home-appliances-for-you/w-vesta-cordless-ironbox-1380W.png','/image/11947?name=11947-1-fw.jpg&wh=200x200' ,'/logout','/login/entryPage=HEADER_ACCOUNT&sourceContext=DEFAULT','/image/11926?name=sm812aaa.jpg&wh=200x200','/logout', '/image/w-vesta-cordless-ironbox-1380W.png']}
plt.plot(df3,'-gD' )
plt.xticks(rotation='vertical')
plt.title("User Access Pattern")
plt.xlabel("Host")
plt.ylabel("Count")
plt.show()

