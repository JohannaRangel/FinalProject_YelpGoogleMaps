# Sentiment Analysis on Google Maps and Yelp Reviews for Ulta Beauty
### Final Project - Henry Bootcamps Data Science
> [!IMPORTANT]
> _This activity (purely educational) corresponds to the final project of Bootcamp Henry - Data Science. It is part of our portfolio of practices that has allowed us to improve Data Science skills with real-world problems and data sets._
> 
![portada](https://github.com/JohannaRangel/ProyectoFinal_YelpGoogleMaps/raw/main/assets/portada.png)

As a data consulting firm, our commitment goes beyond mere data collection. We believe in the transformative power of data, working tirelessly to turn this belief into a tangible reality for our clients. We invite you to explore the structure of this repository to delve deeper into the intricacies of our project. From datasets and preprocessing to analysis and visualization, the repository is a comprehensive resource that provides insight into our methodology and approach.

**Online Reputation:** According to Brightlocal 76% of consumers “regularly” read online reviews when browsing for local businesses. Regarding review platforms in 2022 Google is the most used and trusted platform for reviews followed by Yelp.

<p align="center">
  <img src="https://github.com/JohannaRangel/FinalProject_YelpGoogleMaps/blob/main/assets/platformreviews.png" width="400" alt="platform">
</p>

In this particular project, our experience led us to work hand in hand with **Ulta Beauty**, one of the leading companies in the beauty industry. We were tasked with performing a **Sentiment Analysis on Google Maps and Yelp Reviews**. Extensive exploration of these reviews aimed to uncover valuable insights, improve Ulta Beauty's understanding of customer sentiment, and facilitate data-driven decision making.

At Data Insight Pro, we don't just provide data consulting; we build bridges to the future, where informed decision-making drives business success. Our dedication to excellence and innovation places us at the forefront of the industry, and every project is an opportunity to showcase how data can be the compass guiding businesses toward their objectives.

Thank you for exploring our project repository. Feel free to navigate through the different sections to gain a comprehensive understanding of our methodologies and outcomes. 
> If you have any questions or inquiries, please don't hesitate to reach out.<br /> 
<a href="https://github.com/JohannaRangel/FinalProject_YelpGoogleMaps/wiki#contributors"><img src="https://github.com/JohannaRangel/FinalProject_YelpGoogleMaps/blob/main/assets/contact.png" alt="Contact Us" width="100">

## Repository Structure<br />
├─ [assets/](https://github.com/JohannaRangel/FinalProject_YelpGoogleMaps/tree/main/assets) _Contains multimedia resources, images, or other assets used in the project._<br />
├─ [dashboard/](https://github.com/JohannaRangel/FinalProject_YelpGoogleMaps/tree/main/dashboard) _Includes files related to the creation and development of an interactive dashboard._<br />
├─ [datasets/](https://github.com/JohannaRangel/FinalProject_YelpGoogleMaps/tree/main/datasets) _Stores datasets used in the project, organized into separate folders for csv and parquet files._<br />
├─ [deploy_ml/](https://github.com/JohannaRangel/FinalProject_YelpGoogleMaps/tree/main/deploy_ml) _This folder encompasses all elements related to deploying machine learning models on the Google Cloud. Crucial files such as Dockerfile, main.py, and requirements.txt are located here. You can access the **instructions for deploying Streamlit with Google Run** from this [link](https://github.com/JohannaRangel/FinalProject_YelpGoogleMaps/blob/main/docs/Instructions%20for%20Deploying%20Streamlit%20with%20Google%20Run.md). See **AboutDeployML.md** for more details in this [link](https://github.com/JohannaRangel/FinalProject_YelpGoogleMaps/blob/main/deploy_ml/AboutDeployML.md)_<br />
│  ├─ [csv/](https://github.com/JohannaRangel/FinalProject_YelpGoogleMaps/tree/main/datasets/csv) _Files generated as part of the data analysis._<br />
│  ├─ [parquets/](https://github.com/JohannaRangel/FinalProject_YelpGoogleMaps/tree/main/datasets/parquets) _Containing the raw data provided by Henry as the mandatory starting point for the project._<br />
├─ [diagrams/](https://github.com/JohannaRangel/FinalProject_YelpGoogleMaps/tree/main/diagrams) _This folder contains diagrams and visual schematics related to the data pipeline. Explore graphical representations here that illustrate the architecture, process flows, and other relevant visual aspects of the project._<br /> 
├─ [docs/](https://github.com/JohannaRangel/FinalProject_YelpGoogleMaps/tree/main/docs) _Contains essential documentation for the project such as: guidelines, generalities, evaluation criteria, instructions for implementing MLs, others._<br />
├─ [notebooks/](https://github.com/JohannaRangel/FinalProject_YelpGoogleMaps/tree/main/notebooks) _Here, you will find Jupyter notebooks or other documents related to data exploration and analysis such as ETL/EDA. See **AboutNotebooks.md** for more details in this [link](https://github.com/JohannaRangel/FinalProject_YelpGoogleMaps/blob/main/notebooks/AboutNotebooks.md)._<br />
├─ [sources/](https://github.com/JohannaRangel/FinalProject_YelpGoogleMaps/tree/main/sources) _This folder contains Python scripts and modules related to data processing, utilities, and backend functionality. If you're looking for the Jupyter notebooks or documents related to data exploration and analysis, check folder notebooks._<br /> 
├─ [videos/](https://github.com/JohannaRangel/FinalProject_YelpGoogleMaps/tree/main/videos) _In this folder, files related to visual material are stored, such as tutorials, demonstrations, or any multimedia content in video format that is relevant to the project._<br />

**[Projects](https://github.com/users/JohannaRangel/projects/5)** _Displays project management and ongoing tasks._<br /> 
├─ [Gantt](https://github.com/users/JohannaRangel/projects/5/views/1)<br />
├─ [Kanban](https://github.com/users/JohannaRangel/projects/5/views/2)<br />
├─ [Backlog](https://github.com/users/JohannaRangel/projects/5/views/3)<br />

**[Wiki](https://github.com/JohannaRangel/FinalProject_YelpGoogleMaps/wiki)** _See the Wiki for full documentation._<br /> 
├─ [Guidelines](https://github.com/JohannaRangel/FinalProject_YelpGoogleMaps/wiki/Guidelines)<br />
├─ [Executive Summary](https://github.com/JohannaRangel/FinalProject_YelpGoogleMaps/wiki/Executive%E2%80%90Summary)<br />
├─ [Technical Report](https://github.com/JohannaRangel/FinalProject_YelpGoogleMaps/wiki/Technical%E2%80%90Report)<br />
├─ [References](https://github.com/JohannaRangel/FinalProject_YelpGoogleMaps/wiki/References)<br />
├─ [Conclusions & Acknowledgments](https://github.com/JohannaRangel/FinalProject_YelpGoogleMaps/wiki/Conclusions%E2%80%90Acknowledgments)<br />
├─ [Contributors](https://github.com/JohannaRangel/FinalProject_YelpGoogleMaps/wiki#contributors)<br />
<br />

### **[Streamlit Application](https://endpointmlgcloud-buquga5lhq-uc.a.run.app/ML_-_Detección_de_Tópicos)** :bar_chart:
_Final Products: Two Machine Learning models (**Sentiment Analysis and Topic Detection**) along with an interactive Dashboard featuring 5 key KPIs. Implemented on Google Cloud Platform (GCP) and deployed on Streamlit. Explore the [application](https://endpointmlgcloud-buquga5lhq-uc.a.run.app/ML_-_Detección_de_Tópicos) and enjoy!._ <br />
<br />

### **[Project Presentation: A Comprehensive Insight into Our Innovative Solution](https://www.youtube.com/watch?v=p04NCib2jxQ)** :bar_chart:
_This presentation video offers a concise and professional overview of our final project, highlighting its distinctive features, innovative functionalities (endpoints), and the strategic impact it aims to achieve.Click to watch the [video](https://www.youtube.com/watch?v=p04NCib2jxQ)_ <br />
<br />

**[README.md](https://github.com/JohannaRangel/FinalProject_YelpGoogleMaps/blob/main/README.md)**<br />



![lema](https://github.com/JohannaRangel/FinalProject_YelpGoogleMaps/blob/main/assets/lema.png)
