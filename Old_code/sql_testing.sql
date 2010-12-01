










select DISTINCT f_id, filename, data_level, product_name, process_name from data_files 
join data_products on data_files.dp_id = data_products.dp_id 
join file_processes on data_products.dp_id = file_processes.dp_id 
join processes on file_processes.p_id = processes.p_id 
where file_processes.type=0 and data_files.utc_file_date  < '2010-01-12' ;

select DISTINCT f_id, filename, data_level, product_name, process_name from data_files 
join data_products on data_files.dp_id = data_products.dp_id 
join file_processes on data_products.dp_id = file_processes.dp_id 
join processes on file_processes.p_id = processes.p_id 
where file_processes.type=1 and data_files.utc_file_date  < '2010-01-12' ;


--order by data_files.utc_file_date, data_level;


