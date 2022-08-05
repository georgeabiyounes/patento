from math import radians, cos, sin, asin, sqrt
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
from functools import reduce
import os
from patento import queries as q
from tqdm import tqdm



class BQ(object):

    def __init__(self, PROJECT_ID, PATH_TO_GOOGLE_CLOUD_CREDENTIALS):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = PATH_TO_GOOGLE_CLOUD_CREDENTIALS ## CHANGE THIS TO YOUR OWN CREDENTIALS FILE PATH
        self.bigquery_client = bigquery.Client(project = PROJECT_ID)
        self.storage_client = storage.Client(project = PROJECT_ID)
        print('Successfully connected to project: '+ PROJECT_ID)



    def run_query(self, query, return_results = True):
        if return_results == True:
            df  = self.bigquery_client.query(query).to_dataframe()
            print("Query ran succesfully and returned a dataframe with shape: ",end = "")
            print(df.shape)
            return df
        else: 
            self.bigquery_client.query(query)



    def test_connection(self):
        query  = """
        SELECT * FROM `patents-public-data.patents.publications` LIMIT 1
        """
        df = self.run_query(query, return_results = False)
        print("Connection successful.")
        return df



    def get_bwd_cites(self, publication_numbers):
        list_of_publn_nrs_string = ("['" + "', '".join(publication_numbers) + "']")
        query = """
        WITH mytable AS (
        SELECT ARRAY"""+  list_of_publn_nrs_string +"""  AS publication_number_list)

        SELECT DISTINCT
            t1.publication_number,
            COUNT(DISTINCT t3.application_number) as backward_citations 

        FROM (SELECT publication_number_list_u as publication_number
              FROM `mytable` x1, UNNEST(publication_number_list) as publication_number_list_u) t1     

        LEFT JOIN
            (
            SELECT 
                x2.publication_number AS citing_publication_number, 
                citation_u.publication_number AS cited_publication_number,
                citation_u.category AS cited_publication_category
            FROM 
            `patents-public-data.patents.publications` x2, 
            UNNEST(citation) AS citation_u
        ) t2 
        ON t2.citing_publication_number = t1.publication_number

        LEFT OUTER JOIN `patents-public-data.patents.publications` t3 
        ON t2.cited_publication_number = t3.publication_number 
        
        GROUP BY 
        t1.publication_number
        """
        df_backward_citations = self.run_query(query)
        return df_backward_citations



    def get_fwd_cites(self, publication_numbers, time_window=3):
        list_of_publn_nrs_string = ("['" + "', '".join(publication_numbers) + "']")
        query = """
        WITH mytable AS (
        SELECT ARRAY""" +  list_of_publn_nrs_string + """  AS publication_number_list)
        
        SELECT DISTINCT
        t1.publication_number, 
        
        -- count disctinct application numbers citing our focal patent
        Count(DISTINCT t3.citing_application_number) AS forward_citations 

        FROM (SELECT publication_number_list_u as publication_number
              FROM `mytable` x1, UNNEST(publication_number_list) as publication_number_list_u) t1 
            
        LEFT JOIN (SELECT x2.publication_number, 
                          parse_date('%Y%m%d', CAST(x2.filing_date AS STRING)) as filing_date 
                FROM   `patents-public-data.patents.publications` x2 
                WHERE x2.filing_date != 0) t2
            ON t2.publication_number = t1.publication_number
        
        LEFT JOIN (SELECT 
            
            -- the publication number in the joined table is the citing publication number
            x3.publication_number AS citing_publication_number, 
            
            -- the application number in the joined table is the citing application number
            x3.application_number AS citing_application_number, 

            PARSE_DATE('%Y%m%d', CAST(x3.filing_date AS STRING)) AS joined_filing_date,
            
            -- the publication number in the unnested citation record is the cited publication number
            citation_u.publication_number AS cited_publication_number 
            
            FROM `patents-public-data.patents.publications` x3, UNNEST(citation) AS citation_u
            WHERE x3.filing_date!=0) t3 
            
            -- joining our focal publication number on cited publication number 
            ON t1.publication_number = t3.cited_publication_number 
            AND t3.joined_filing_date BETWEEN  t2.filing_date AND DATE_ADD(t2.filing_date, INTERVAL """ + str(time_window)+ """ YEAR)
    
        GROUP BY 
        t1.publication_number
        ORDER BY 
        t1.publication_number
        """
        df_forward_citations = self.run_query(query)
        return df_forward_citations



    def get_famsize(self, publication_numbers):
        list_of_publn_nrs_string = ("['" + "', '".join(publication_numbers) + "']")
        query = """
        WITH mytable AS (
            SELECT 
                ARRAY """ +  list_of_publn_nrs_string + """ AS publication_number_list
            ) 

        SELECT DISTINCT
            t1.publication_number, 
            -- count distinct application numbers sharing same family id with our focal patents
            Count(DISTINCT t3.application_number) AS family_size 
        FROM 
            (
                SELECT 
                publication_number_list_u as publication_number 
                FROM 
                `mytable` x1, 
                UNNEST(publication_number_list) as publication_number_list_u
            ) t1 

        LEFT JOIN `patents-public-data.patents.publications` t2 ON t1.publication_number = t2.publication_number 

        LEFT JOIN `patents-public-data.patents.publications` t3 ON t2.family_id = t3.family_id 

        GROUP BY 
            t1.publication_number
        """
        df_famsize = self.run_query(query)
        return df_famsize




    def get_geofamsize(self, publication_numbers):
        list_of_publn_nrs_string = ("['" + "', '".join(publication_numbers) + "']")
        query = """
        WITH mytable AS (
            SELECT 
                ARRAY """ +  list_of_publn_nrs_string + """ AS publication_number_list
            )
        
        SELECT
               t1.publication_number,
               COUNT(DISTINCT t3.country_code) AS geog_family_size,
        
        FROM 
            (
                SELECT 
                publication_number_list_u as publication_number 
                FROM 
                `mytable` x1, 
                UNNEST(publication_number_list) as publication_number_list_u
            ) t1 

        LEFT JOIN `patents-public-data.patents.publications` t2  ON t1.publication_number = t2.publication_number

        LEFT JOIN `patents-public-data.patents.publications` t3  ON t2.family_id = t3.family_id

        WHERE  
            t3.country_code != 'WO'
        
        GROUP  BY 
            t1.publication_number 
        """
        df_geofamsize = self.run_query(query)
        return df_geofamsize




    def get_claims_count(self, publication_numbers):
        list_of_publn_nrs_string = ("['" + "', '".join(publication_numbers) + "']")
        query = """
        WITH mytable AS (
            SELECT 
                ARRAY """ +  list_of_publn_nrs_string + """ AS publication_number_list
            )
        
        SELECT DISTINCT
            t1.publication_number,
            COUNT(t2.claims) AS nb_claims,
            COUNTIF(t2.claims NOT LIKE '%claim%') AS nb_indep_claims,
            COUNT(t2.claims) - COUNTIF(t2.claims NOT LIKE '%claim%') AS nb_dep_claims

        FROM 
            (
                SELECT 
                    publication_number_list_u as publication_number 
                FROM 
                    `mytable` x1, 
                    UNNEST(publication_number_list) as publication_number_list_u
            ) t1 

        LEFT JOIN
            (
                SELECT 
                    x1.publication_number,
                    claims
                FROM   
                    `usptobias.tmp_george.cleaned_claims` x1,
                    unnest(cleaned_claims) AS claims
            ) t2
        ON  t2.publication_number = t1.publication_number

        GROUP BY  
            t1.publication_number
        """
        df_claims_count = self.run_query(query)
        return df_claims_count




    def get_claims(self, publication_numbers):
        list_of_publn_nrs_string = ("['" + "', '".join(publication_numbers) + "']")
        query = """
        WITH mytable AS (
            SELECT 
                ARRAY """ +  list_of_publn_nrs_string + """ AS publication_number_list
            )
        
        SELECT    
            t1.publication_number,
            t2.cleaned_claims

        FROM 
            (
                SELECT 
                    publication_number_list_u as publication_number 
                FROM 
                    `mytable` x1, 
                    UNNEST(publication_number_list) as publication_number_list_u
            ) t1 

        LEFT JOIN  `usptobias.tmp_george.cleaned_claims` t2 ON t2.publication_number = t1.publication_number
        """
        df_claims= self.run_query(query)
        return df_claims


    def get_originality(self, publication_numbers):
        list_of_publn_nrs_string = ("['" + "', '".join(publication_numbers) + "']")
        query = """
        WITH mytable AS (
            SELECT
                ARRAY """ +  list_of_publn_nrs_string + """ AS publication_number_list
            )
        
        SELECT u1.publication_number, 
       -- calculates originality
            1-SUM(POWER(ipc_occurrences,2))/POWER(SUM(ipc_occurrences),2) as originality 
        FROM

        -- table u1 contains the number of occurrences of the ipc4 codes of the backward citations of focal patent
        (
            SELECT t1.publication_number, 
                t3.ipc4, 
                count(t3.ipc4) as ipc_occurrences 
            FROM (
                SELECT 
                    publication_number_list_u as publication_number 
                FROM 
                    `mytable` x1, 
                    UNNEST(publication_number_list) as publication_number_list_u
            ) t1 
            
            -- joins backward citations 
            LEFT JOIN (SELECT x2.publication_number AS citing_publication_number, 
                            citation_u.publication_number AS backward_citation 
                    FROM `patents-public-data.patents.publications` x2, 
                            UNNEST(citation) as citation_u) t2 
                    ON t2.citing_publication_number = t1.publication_number
                    
            -- joins 4-digit ipc codes of backward citations
            LEFT JOIN `usptobias.tmp_george.chosen_ipc4_view` t3 
            ON t3.publication_number = t2.backward_citation

            GROUP BY t1.publication_number, 
                    t3.ipc4) u1

        GROUP BY u1.publication_number

        -- filters out patents with no backward citations
        HAVING SUM(ipc_occurrences)>0
        """
        df_claims= self.run_query(query)
        return df_claims


    def get_inventors_count(self, publication_numbers):
        list_of_publn_nrs_string = ("['" + "', '".join(publication_numbers) + "']")
        query = """
        WITH mytable AS (
            SELECT
                ARRAY """ +  list_of_publn_nrs_string + """ AS publication_number_list
            )
        
        SELECT t1.publication_number, 
        ARRAY_LENGTH(t2.inventor) as count_inventors 

        FROM (
                SELECT 
                    publication_number_list_u as publication_number 
                FROM 
                    `mytable` x1, 
                    UNNEST(publication_number_list) as publication_number_list_u
            ) t1
            
        LEFT JOIN `patents-public-data.patents.publications` t2 
        ON t2.publication_number = t1.publication_number
        ORDER BY t1.publication_number
        """
        df_claims= self.run_query(query)
        return df_claims


    def get_ipc_count(self, publication_numbers):
        list_of_publn_nrs_string = ("['" + "', '".join(publication_numbers) + "']")
        query = """
        WITH mytable AS (
            SELECT
                ARRAY """ +  list_of_publn_nrs_string + """ AS publication_number_list
            )

        SELECT t1.publication_number, 
               ARRAY_LENGTH(ipc) as count_ipc
        FROM (
                SELECT 
                    publication_number_list_u as publication_number 
                FROM 
                    `mytable` x1, 
                    UNNEST(publication_number_list) as publication_number_list_u
            ) t1

        LEFT JOIN `patents-public-data.patents.publications` t2 
        ON t2.publication_number = t1.publication_number
        """
        df_claims= self.run_query(query)
        return df_claims        


    def get_rejections_count(self, publication_numbers):
        list_of_publn_nrs_string = ("['" + "', '".join(publication_numbers) + "']")
        query = """
        WITH mytable AS (
            SELECT
                ARRAY """ +  list_of_publn_nrs_string + """ AS publication_number_list
            )

        SELECT t1.publication_number, 
        COUNTIF(t4.event_code = 'CTNF') as count_rejections, 
        MIN(t5.recorded_date) as expiry_date

        FROM (
                SELECT 
                    publication_number_list_u as publication_number 
                FROM 
                    `mytable` x1, 
                    UNNEST(publication_number_list) as publication_number_list_u
            ) t1

        LEFT JOIN `patents-public-data.patents.publications` t2 
        ON t2.publication_number = t1.publication_number

        LEFT JOIN `patents-public-data.uspto_oce_pair.match` t3 
        ON t3.application_number = t2.application_number

        LEFT JOIN `patents-public-data.uspto_oce_pair.transactions` t4 
        ON t4.application_number = t3.application_number_pair

        LEFT OUTER JOIN `patents-public-data.uspto_oce_pair.transactions` t5 
        ON t5.application_number = t3.application_number_pair
        AND t5.event_code = 'EXP.'
        GROUP BY t1.publication_number

        """
        df_claims= self.run_query(query)
        return df_claims 

    
    def get_applicants_count(self, publication_numbers):
        list_of_publn_nrs_string = ("['" + "', '".join(publication_numbers) + "']")
        query = """
        WITH mytable AS (
            SELECT
                ARRAY """ +  list_of_publn_nrs_string + """ AS publication_number_list
            )
    
        SELECT t1.publication_number, 
               ARRAY_LENGTH(t2.assignee) as nb_assignees

        FROM (
                SELECT 
                    publication_number_list_u as publication_number 
                FROM 
                    `mytable` x1, 
                    UNNEST(publication_number_list) as publication_number_list_u
            ) t1

        LEFT JOIN `patents-public-data.patents.publications` t2 
        ON t2.publication_number = t1.publication_number

        ORDER BY t1.publication_number
        """
        df_claims= self.run_query(query)
        return df_claims


    def get_priority_status(self, publication_numbers):
        list_of_publn_nrs_string = ("['" + "', '".join(publication_numbers) + "']")
        query = """
        WITH mytable AS (
            SELECT
                ARRAY """ +  list_of_publn_nrs_string + """ AS publication_number_list
            )

        SELECT t1.publication_number,
               t3.application_number, 
               ARRAY_AGG(t2.priority_claim_u.application_number IGNORE NULLS) as priority_applications, 
               t3.application_number IN UNNEST(ARRAY_AGG(t2.priority_claim_u.application_number)) as priority_filing 

        FROM (
                SELECT 
                    publication_number_list_u as publication_number 
                FROM 
                    `mytable` x1, 
                    UNNEST(publication_number_list) as publication_number_list_u
            ) t1 

        LEFT JOIN `patents-public-data.patents.publications` t3 
        ON t3.publication_number = t1.publication_number

        LEFT JOIN (
            SELECT 
            x2.publication_number, 
            priority_claim_u 
            FROM 
            `patents-public-data.patents.publications` x2, 
            UNNEST(priority_claim) as priority_claim_u
        ) t2 
        ON t1.publication_number = t2.publication_number 

        GROUP BY t3.application_number, t1.publication_number
        """
        df_claims= self.run_query(query)
        return df_claims


    def get_filing_granting_dates(self, publication_numbers):
        list_of_publn_nrs_string = ("['" + "', '".join(publication_numbers) + "']")
        query = """
        WITH mytable AS (
            SELECT
                ARRAY """ +  list_of_publn_nrs_string + """ AS publication_number_list
            )

        SELECT t1.publication_number,
               PARSE_DATE('%Y%m%d', CAST(t2.grant_date AS STRING)) as grant_date,
               PARSE_DATE('%Y%m%d', CAST(t2.filing_date AS STRING)) as filing_date,
               DATE_DIFF(PARSE_DATE('%Y%m%d', CAST(t2.grant_date AS STRING)), 
               PARSE_DATE('%Y%m%d', CAST(t2.filing_date AS STRING)), MONTH) as grant_lag_months

        FROM (
                SELECT 
                    publication_number_list_u as publication_number 
                FROM 
                    `mytable` x1, 
                    UNNEST(publication_number_list) as publication_number_list_u
            ) t1 

        LEFT JOIN `patents-public-data.patents.publications` t2 
        ON t2.publication_number = t1.publication_number
        """
        df_claims= self.run_query(query)
        return df_claims


    def get_npl_citations_count(self, publication_numbers):
        list_of_publn_nrs_string = ("['" + "', '".join(publication_numbers) + "']")
        query = """
        WITH mytable AS (
            SELECT
                ARRAY """ +  list_of_publn_nrs_string + """ AS publication_number_list
            )

        SELECT t1.publication_number,
               COUNT(DISTINCT t2.patcit_id) as nb_npl

        FROM (
                SELECT 
                    publication_number_list_u as publication_number 
                FROM 
                    `mytable` x1, 
                    UNNEST(publication_number_list) as publication_number_list_u
            ) t1

        LEFT JOIN (SELECT x1.patcit_id, 
                          cited_by_u.publication_number as citing_publication_number 
                    FROM `patcit-public-data.frontpage.bibliographical_reference` x1, 
                          UNNEST(cited_by) as cited_by_u) t2
        ON t2.citing_publication_number = t1.publication_number

        GROUP BY t1.publication_number 
        """
        df_claims= self.run_query(query)
        return df_claims


    def get_process_status(self, publication_numbers):
        list_of_publn_nrs_string = ("['" + "', '".join(publication_numbers) + "']")
        query = """
        WITH mytable AS (
            SELECT
                ARRAY """ +  list_of_publn_nrs_string + """ AS publication_number_list
            )

        SELECT t1.publication_number,
        MAX(process) as is_process

        FROM (
                SELECT 
                    publication_number_list_u as publication_number 
                FROM 
                    `mytable` x1, 
                    UNNEST(publication_number_list) as publication_number_list_u
            ) t1

        LEFT JOIN 
        (SELECT x1.publication_number,
                            claims_u,
                            IF(REGEXP_CONTAINS(LOWER(claims_u), r'process|method'), 1,0) as process
                        FROM   `usptobias.paper_1_george.cleaned_claims` x1,
                                unnest(cleaned_claims) AS claims_u) t2
        ON        t2.publication_number = t1.publication_number
        GROUP BY t1.publication_number
"""
        df_claims= self.run_query(query)
        return df_claims


    def get_indicators(self, publication_numbers):
        list_of_publn_nrs_string = ("['" + "', '".join(publication_numbers) + "']")
        df_list = [
                   self.get_bwd_cites(publication_numbers),
                   self.get_fwd_cites(publication_numbers), 
                   self.get_famsize(publication_numbers),
                   self.get_geofamsize(publication_numbers),
                   self.get_claims(publication_numbers),
                   self.get_claims_count(publication_numbers),
                   self.get_originality(publication_numbers),
                   self.get_inventors_count(publication_numbers),
                   self.get_ipc_count(publication_numbers),
                   self.get_rejections_count(publication_numbers),
                   self.get_applicants_count(publication_numbers),
                   self.get_priority_status(publication_numbers),
                   self.get_filing_granting_dates(publication_numbers),
                   self.get_npl_citations_count(publication_numbers),
                   self.get_process_status(publication_numbers)
                   ]
        
        df = reduce(lambda df1,df2: pd.merge(df1,df2,on='publication_number'), df_list)
        return df