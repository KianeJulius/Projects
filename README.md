# Querying On AWS Athena

All AWS API keys are masked with "###"

```python
# pip install pyathena
```


```python
# pip install PyAthena[Pandas]
```

import sqlalchemy


```python
from urllib.parse import quote_plus
from sqlalchemy.engine import create_engine
from sqlalchemy.sql.expression import select
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.schema import Table, MetaData
import sqlalchemy
```

import athena


```python
from pyathena import connect
```


```python
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
%matplotlib inline
%config InlineBackend.figure_format='retina'
import seaborn as sns
```


```python
database = 'dsoaws'
table = 'amazon_reviews_tsv'
bucket = 'data-science-on-aws22'
```

create connection engine


```python
engine = create_engine("awsathena+rest://###:###@athena.us-east-1.amazonaws.com:443/"\
                       "default?s3_staging_dir=s3://data-science-on-aws22/athena/staging/")
```


```python
# conn_str = "awsathena+rest://{aws_access_key_id}:{aws_secret_access_key}@athena.us-east-1.amazonaws.com:443/"\
#            "{schema_name}?s3_staging_dir={s3_staging_dir}"

# engine = create_engine(conn_str.format(
#     aws_access_key_id=quote_plus("###"),
#     aws_secret_access_key=quote_plus("###"), 
#     region_name="us-east-1", 
#     schema_name="default",
#     s3_staging_dir=quote_plus("s3://{0}/path/to/").format(bucket)))
#     session_token = 'kiane'
```

the sql statement


```python
sql_statement="""
SELECT DISTINCT product_category from {0}.{1} 
ORDER BY product_category 
""".format(database,table)
```


```python
pd.read_sql(sql_statement, con=engine)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>product_category</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Apparel</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Automotive</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Baby</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Beauty</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Books</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Camera</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Digital_Ebook_Purchase</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Digital_Music_Purchase</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Digital_Software</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Digital_Video_Download</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Digital_Video_Games</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Electronics</td>
    </tr>
    <tr>
      <th>12</th>
      <td>Furniture</td>
    </tr>
    <tr>
      <th>13</th>
      <td>Gift Card</td>
    </tr>
    <tr>
      <th>14</th>
      <td>Grocery</td>
    </tr>
    <tr>
      <th>15</th>
      <td>Health &amp; Personal Care</td>
    </tr>
    <tr>
      <th>16</th>
      <td>Home</td>
    </tr>
    <tr>
      <th>17</th>
      <td>Home Entertainment</td>
    </tr>
    <tr>
      <th>18</th>
      <td>Home Improvement</td>
    </tr>
    <tr>
      <th>19</th>
      <td>Jewelry</td>
    </tr>
    <tr>
      <th>20</th>
      <td>Kitchen</td>
    </tr>
    <tr>
      <th>21</th>
      <td>Lawn and Garden</td>
    </tr>
    <tr>
      <th>22</th>
      <td>Luggage</td>
    </tr>
    <tr>
      <th>23</th>
      <td>Major Appliances</td>
    </tr>
    <tr>
      <th>24</th>
      <td>Mobile_Apps</td>
    </tr>
    <tr>
      <th>25</th>
      <td>Mobile_Electronics</td>
    </tr>
    <tr>
      <th>26</th>
      <td>Music</td>
    </tr>
    <tr>
      <th>27</th>
      <td>Musical Instruments</td>
    </tr>
    <tr>
      <th>28</th>
      <td>Office Products</td>
    </tr>
    <tr>
      <th>29</th>
      <td>Outdoors</td>
    </tr>
    <tr>
      <th>30</th>
      <td>PC</td>
    </tr>
    <tr>
      <th>31</th>
      <td>Personal_Care_Appliances</td>
    </tr>
    <tr>
      <th>32</th>
      <td>Pet Products</td>
    </tr>
    <tr>
      <th>33</th>
      <td>Shoes</td>
    </tr>
    <tr>
      <th>34</th>
      <td>Software</td>
    </tr>
    <tr>
      <th>35</th>
      <td>Sports</td>
    </tr>
    <tr>
      <th>36</th>
      <td>Tools</td>
    </tr>
    <tr>
      <th>37</th>
      <td>Toys</td>
    </tr>
    <tr>
      <th>38</th>
      <td>Video</td>
    </tr>
    <tr>
      <th>39</th>
      <td>Video DVD</td>
    </tr>
    <tr>
      <th>40</th>
      <td>Video Games</td>
    </tr>
    <tr>
      <th>41</th>
      <td>Watches</td>
    </tr>
    <tr>
      <th>42</th>
      <td>Wireless</td>
    </tr>
  </tbody>
</table>
</div>



##  Which product categories are the highest rated by average rating?


```python
sql2 = """SELECT product_category, AVG(star_rating) AS avg_star_rating
FROM {0}.{1} 
GROUP BY product_category 
ORDER BY avg_star_rating DESC
""".format(database,table)
```


```python
pd.read_sql(sql2, con=engine)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>product_category</th>
      <th>avg_star_rating</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Gift Card</td>
      <td>4.731363</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Digital_Music_Purchase</td>
      <td>4.642891</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Music</td>
      <td>4.436624</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Books</td>
      <td>4.341658</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Grocery</td>
      <td>4.312219</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Digital_Ebook_Purchase</td>
      <td>4.308775</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Video DVD</td>
      <td>4.302017</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Tools</td>
      <td>4.261769</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Musical Instruments</td>
      <td>4.251103</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Automotive</td>
      <td>4.246302</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Shoes</td>
      <td>4.241260</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Outdoors</td>
      <td>4.240019</td>
    </tr>
    <tr>
      <th>12</th>
      <td>Sports</td>
      <td>4.229365</td>
    </tr>
    <tr>
      <th>13</th>
      <td>Luggage</td>
      <td>4.223391</td>
    </tr>
    <tr>
      <th>14</th>
      <td>Toys</td>
      <td>4.211735</td>
    </tr>
    <tr>
      <th>15</th>
      <td>Kitchen</td>
      <td>4.207424</td>
    </tr>
    <tr>
      <th>16</th>
      <td>Digital_Video_Download</td>
      <td>4.201208</td>
    </tr>
    <tr>
      <th>17</th>
      <td>Video</td>
      <td>4.191511</td>
    </tr>
    <tr>
      <th>18</th>
      <td>Beauty</td>
      <td>4.187224</td>
    </tr>
    <tr>
      <th>19</th>
      <td>Home Improvement</td>
      <td>4.182270</td>
    </tr>
    <tr>
      <th>20</th>
      <td>Home</td>
      <td>4.178399</td>
    </tr>
    <tr>
      <th>21</th>
      <td>Baby</td>
      <td>4.162683</td>
    </tr>
    <tr>
      <th>22</th>
      <td>Health &amp; Personal Care</td>
      <td>4.161833</td>
    </tr>
    <tr>
      <th>23</th>
      <td>Jewelry</td>
      <td>4.144090</td>
    </tr>
    <tr>
      <th>24</th>
      <td>Pet Products</td>
      <td>4.143653</td>
    </tr>
    <tr>
      <th>25</th>
      <td>Watches</td>
      <td>4.138283</td>
    </tr>
    <tr>
      <th>26</th>
      <td>Camera</td>
      <td>4.127015</td>
    </tr>
    <tr>
      <th>27</th>
      <td>Apparel</td>
      <td>4.105229</td>
    </tr>
    <tr>
      <th>28</th>
      <td>Lawn and Garden</td>
      <td>4.093177</td>
    </tr>
    <tr>
      <th>29</th>
      <td>PC</td>
      <td>4.086444</td>
    </tr>
    <tr>
      <th>30</th>
      <td>Furniture</td>
      <td>4.083949</td>
    </tr>
    <tr>
      <th>31</th>
      <td>Office Products</td>
      <td>4.072539</td>
    </tr>
    <tr>
      <th>32</th>
      <td>Video Games</td>
      <td>4.059893</td>
    </tr>
    <tr>
      <th>33</th>
      <td>Electronics</td>
      <td>4.035507</td>
    </tr>
    <tr>
      <th>34</th>
      <td>Mobile_Apps</td>
      <td>3.981594</td>
    </tr>
    <tr>
      <th>35</th>
      <td>Personal_Care_Appliances</td>
      <td>3.977402</td>
    </tr>
    <tr>
      <th>36</th>
      <td>Home Entertainment</td>
      <td>3.902123</td>
    </tr>
    <tr>
      <th>37</th>
      <td>Wireless</td>
      <td>3.891779</td>
    </tr>
    <tr>
      <th>38</th>
      <td>Digital_Video_Games</td>
      <td>3.853126</td>
    </tr>
    <tr>
      <th>39</th>
      <td>Mobile_Electronics</td>
      <td>3.763163</td>
    </tr>
    <tr>
      <th>40</th>
      <td>Major Appliances</td>
      <td>3.716185</td>
    </tr>
    <tr>
      <th>41</th>
      <td>Software</td>
      <td>3.567035</td>
    </tr>
    <tr>
      <th>42</th>
      <td>Digital_Software</td>
      <td>3.539330</td>
    </tr>
  </tbody>
</table>
</div>




```python
result = pd.read_sql(sql2, con=engine)
```

Set the size of plot canvas


```python
plt.rcParams['figure.figsize'] = [10, 10]
```


```python
plt.barh(result['product_category'],result['avg_star_rating'], color ='green')
 
plt.xlabel("Product Category")
plt.ylabel("Average Rating")
plt.show()
```


    
![png](output_21_0.png)
    


## Which product categories have the most reviews?


```python
sql3 = """SELECT product_category, COUNT(star_rating) AS count_star_rating
FROM {0}.{1} 
GROUP BY product_category 
ORDER BY count_star_rating
""".format(database,table)

result2 = pd.read_sql(sql3, con=engine)
```


```python
plt.barh(result2['product_category'],result2['count_star_rating'], color ='green')
 
plt.xlabel("Product Category")
plt.ylabel("Number of Ratings")
plt.show()
```


    
![png](output_24_0.png)
    


## When did each product category become available in the Amazon catalog

I need to check first the column schema


```python
sql_test = """SELECT *
FROM {0}.{1} 
LIMIT 3
""".format(database,table)

result_all = pd.read_sql(sql_test, con=engine)
result_all
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>marketplace</th>
      <th>customer_id</th>
      <th>review_id</th>
      <th>product_id</th>
      <th>product_parent</th>
      <th>product_title</th>
      <th>product_category</th>
      <th>star_rating</th>
      <th>helpful_votes</th>
      <th>total_votes</th>
      <th>vine</th>
      <th>verified_purchase</th>
      <th>review_headline</th>
      <th>review_body</th>
      <th>review_date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>US</td>
      <td>12076615</td>
      <td>RQ58W7SMO911M</td>
      <td>0385730586</td>
      <td>122662979</td>
      <td>Sisterhood of the Traveling Pants (Book 1)</td>
      <td>Books</td>
      <td>4</td>
      <td>2</td>
      <td>3</td>
      <td>N</td>
      <td>N</td>
      <td>this book was a great learning novel!</td>
      <td>this boook was a great one that you could lear...</td>
      <td>2005-10-14</td>
    </tr>
    <tr>
      <th>1</th>
      <td>US</td>
      <td>12703090</td>
      <td>RF6IUKMGL8SF</td>
      <td>0811828964</td>
      <td>56191234</td>
      <td>The Bad Girl's Guide to Getting What You Want</td>
      <td>Books</td>
      <td>3</td>
      <td>5</td>
      <td>5</td>
      <td>N</td>
      <td>N</td>
      <td>Fun Fluff</td>
      <td>If you are looking for something to stimulate ...</td>
      <td>2005-10-14</td>
    </tr>
    <tr>
      <th>2</th>
      <td>US</td>
      <td>12257412</td>
      <td>R1DOSHH6AI622S</td>
      <td>1844161560</td>
      <td>253182049</td>
      <td>Eisenhorn (A Warhammer 40,000 Omnibus)</td>
      <td>Books</td>
      <td>4</td>
      <td>1</td>
      <td>22</td>
      <td>N</td>
      <td>N</td>
      <td>this isn't a review</td>
      <td>never read it-a young relative idicated he lik...</td>
      <td>2005-10-14</td>
    </tr>
  </tbody>
</table>
</div>



Then run the query after investigation.


```python
sql4 = """SELECT product_category, MIN(EXTRACT(YEAR FROM CAST(review_date AS DATE))) AS release_year
FROM {0}.{1} 
GROUP BY product_category 
""".format(database,table)

result4 = pd.read_sql(sql4, con=engine)
result4
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>product_category</th>
      <th>release_year</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Wireless</td>
      <td>1998</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Personal_Care_Appliances</td>
      <td>2000</td>
    </tr>
    <tr>
      <th>2</th>
      <td>PC</td>
      <td>1999</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Home Improvement</td>
      <td>1999</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Mobile_Apps</td>
      <td>2010</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Gift Card</td>
      <td>2004</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Tools</td>
      <td>1999</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Beauty</td>
      <td>2000</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Automotive</td>
      <td>1999</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Major Appliances</td>
      <td>2000</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Books</td>
      <td>1995</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Office Products</td>
      <td>1998</td>
    </tr>
    <tr>
      <th>12</th>
      <td>Musical Instruments</td>
      <td>1999</td>
    </tr>
    <tr>
      <th>13</th>
      <td>Digital_Music_Purchase</td>
      <td>2000</td>
    </tr>
    <tr>
      <th>14</th>
      <td>Digital_Ebook_Purchase</td>
      <td>1999</td>
    </tr>
    <tr>
      <th>15</th>
      <td>Electronics</td>
      <td>1999</td>
    </tr>
    <tr>
      <th>16</th>
      <td>Sports</td>
      <td>1997</td>
    </tr>
    <tr>
      <th>17</th>
      <td>Home</td>
      <td>1998</td>
    </tr>
    <tr>
      <th>18</th>
      <td>Mobile_Electronics</td>
      <td>2001</td>
    </tr>
    <tr>
      <th>19</th>
      <td>Baby</td>
      <td>1999</td>
    </tr>
    <tr>
      <th>20</th>
      <td>Digital_Software</td>
      <td>2008</td>
    </tr>
    <tr>
      <th>21</th>
      <td>Jewelry</td>
      <td>2001</td>
    </tr>
    <tr>
      <th>22</th>
      <td>Music</td>
      <td>1995</td>
    </tr>
    <tr>
      <th>23</th>
      <td>Digital_Video_Games</td>
      <td>2006</td>
    </tr>
    <tr>
      <th>24</th>
      <td>Health &amp; Personal Care</td>
      <td>1999</td>
    </tr>
    <tr>
      <th>25</th>
      <td>Video Games</td>
      <td>1997</td>
    </tr>
    <tr>
      <th>26</th>
      <td>Furniture</td>
      <td>2000</td>
    </tr>
    <tr>
      <th>27</th>
      <td>Video</td>
      <td>1995</td>
    </tr>
    <tr>
      <th>28</th>
      <td>Luggage</td>
      <td>2002</td>
    </tr>
    <tr>
      <th>29</th>
      <td>Shoes</td>
      <td>1999</td>
    </tr>
    <tr>
      <th>30</th>
      <td>Home Entertainment</td>
      <td>1998</td>
    </tr>
    <tr>
      <th>31</th>
      <td>Outdoors</td>
      <td>1999</td>
    </tr>
    <tr>
      <th>32</th>
      <td>Apparel</td>
      <td>2000</td>
    </tr>
    <tr>
      <th>33</th>
      <td>Camera</td>
      <td>1998</td>
    </tr>
    <tr>
      <th>34</th>
      <td>Pet Products</td>
      <td>1998</td>
    </tr>
    <tr>
      <th>35</th>
      <td>Lawn and Garden</td>
      <td>1999</td>
    </tr>
    <tr>
      <th>36</th>
      <td>Digital_Video_Download</td>
      <td>2000</td>
    </tr>
    <tr>
      <th>37</th>
      <td>Video DVD</td>
      <td>1996</td>
    </tr>
    <tr>
      <th>38</th>
      <td>Software</td>
      <td>1998</td>
    </tr>
    <tr>
      <th>39</th>
      <td>Kitchen</td>
      <td>2000</td>
    </tr>
    <tr>
      <th>40</th>
      <td>Watches</td>
      <td>2001</td>
    </tr>
    <tr>
      <th>41</th>
      <td>Toys</td>
      <td>1997</td>
    </tr>
    <tr>
      <th>42</th>
      <td>Grocery</td>
      <td>1999</td>
    </tr>
  </tbody>
</table>
</div>



## What is the breakdown of star ratings (1–5) per product category?


```python
sql5 = """SELECT product_category, star_rating, COUNT(*) AS count_reviews
FROM {0}.{1} 
GROUP BY product_category, star_rating
ORDER BY product_category ASC, star_rating DESC,count_reviews
""".format(database,table)

result5 = pd.read_sql(sql5, con=engine)
result5
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>product_category</th>
      <th>star_rating</th>
      <th>count_reviews</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Apparel</td>
      <td>5</td>
      <td>3320566</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Apparel</td>
      <td>4</td>
      <td>1147237</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Apparel</td>
      <td>3</td>
      <td>623471</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Apparel</td>
      <td>2</td>
      <td>369601</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Apparel</td>
      <td>1</td>
      <td>445458</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>210</th>
      <td>Wireless</td>
      <td>5</td>
      <td>4824783</td>
    </tr>
    <tr>
      <th>211</th>
      <td>Wireless</td>
      <td>4</td>
      <td>1501327</td>
    </tr>
    <tr>
      <th>212</th>
      <td>Wireless</td>
      <td>3</td>
      <td>815205</td>
    </tr>
    <tr>
      <th>213</th>
      <td>Wireless</td>
      <td>2</td>
      <td>598330</td>
    </tr>
    <tr>
      <th>214</th>
      <td>Wireless</td>
      <td>1</td>
      <td>1262376</td>
    </tr>
  </tbody>
</table>
<p>215 rows × 3 columns</p>
</div>




```python
sql5_2 = """SELECT star_rating, COUNT(*) AS count_reviews,
FROM {0}.{1} 
GROUP BY star_rating
ORDER BY star_rating DESC
""".format(database,table)

result5_2 = pd.read_sql(sql5_2, con=engine)
result5_2
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>star_rating</th>
      <th>count_reviews</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>5</td>
      <td>93200812</td>
    </tr>
    <tr>
      <th>1</th>
      <td>4</td>
      <td>26223470</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>12133927</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2</td>
      <td>7304430</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1</td>
      <td>12099639</td>
    </tr>
  </tbody>
</table>
</div>

