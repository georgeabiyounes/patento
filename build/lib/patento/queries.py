query_bwd_cites = """
        WITH mytable AS (
        SELECT ARRAY AS publication_number_list)

        SELECT 
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

query_fwd_cites_3y = query = """
        WITH mytable AS (
        SELECT ARRAY AS publication_number_list)
        
        SELECT 
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
            AND t3.joined_filing_date BETWEEN  t2.filing_date AND DATE_ADD(t2.filing_date, INTERVAL 3 YEAR)
    
        GROUP BY 
        t1.publication_number
        ORDER BY 
        t1.publication_number
        """