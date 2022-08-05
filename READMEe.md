# Statement

The purpose of this project is to help researchers easily access patent indicators. This is done through a number of functions 
that calculate and fetch certain patent for a given set of publication numbers. The project uses Google Patents Public Data hosted 
on Google Cloud Platform (GCP) by querying and wrangling data sourced from its tables. The user will need to connect to their GCP account 
and can easily do so through the provided functions, as explained below.


> <font color='orange'>Version 0.1: This is a development release. Some features might be changed in backward-incompatible ways.</font>

# Documentation

[![alt text](https://gitlab.com/uploads/-/system/project/avatar/1058960/gitbook.png "Logo GitBook")][GBOP]

Please, visit our [GitBook][GBOP] for full documentation, examples and resources.   

## Views and Plots


Readers interested in the views and plots generated in the course of the Exploratory Data Analysis (`EDA/*.ipynb`) can access them on our dedicated dropbox. 


[![alt text][db-logo]][db]


# Installation 

## Clone/ Download the openPatstat repository

### Git

```bash
cd destination/path
git clone https://github.com/cverluise/openPatstat.git
````

### Download

1. Go to the [Open Patstat GitHub repository][GHOP]​
2. Click `Clone` or Download (top right)
3. Click `Download ZIP`

## Install the open_pastat python module

```bash
cd path/to/open_patstat
pip install -r requirements.txt
pip install -e .
```
