Useful Links:
-
- [Airflow Lab Document](https://docs.google.com/document/d/1TT9DPc1VNnBIkICwwdtQRbHxTpzWPOUc2e4DddDoEwQ/edit)
- [Big Data Project](https://docs.google.com/document/d/1X6sWnlEw4DDjK8kzjYkLYlx5U8y6rsKkhCkMwJPqzNU/edit#heading=h.7pmicour1pa6)
- [Data Lake Docs](https://docs.google.com/presentation/d/1DHh1KYhu9wqWWhh8zBUT5-junSA1cq3g_9xFbEbNiDc/edit#slide=id.g4f98ee6c26_0_2187)

Useful Commands(WSL):
-
```shell
source .venv/bin/activate
export AIRFLOW_HOME=/home/orionvi/PycharmProjects/data-lake-project-2024/airflow_installation
export PYTHONPATH=/home/orionvi/PycharmProjects/data-lake-project-2024/.venv/bin/
airflow standalone
```

Options for local distributed storage:
-
- [MinIO](https://min.io/docs/minio/linux/developers/python/minio-py.html)

Elastic search Security:
-
```
--------------------------- Security autoconfiguration information ------------------------------

Authentication and authorization are enabled.
TLS for the transport and HTTP layers is enabled and configured.

The generated password for the elastic built-in superuser is : -afhBk97FYkWmUIO_XHj

If this node should join an existing cluster, you can reconfigure this with
'/usr/share/elasticsearch/bin/elasticsearch-reconfigure-node --enrollment-token <token-here>'
after creating an enrollment token on your existing cluster.

You can complete the following actions at any time:

Reset the password of the elastic built-in superuser with 
'/usr/share/elasticsearch/bin/elasticsearch-reset-password -u elastic'.

Generate an enrollment token for Kibana instances with 
 '/usr/share/elasticsearch/bin/elasticsearch-create-enrollment-token -s kibana'.

Generate an enrollment token for Elasticsearch nodes with 
'/usr/share/elasticsearch/bin/elasticsearch-create-enrollment-token -s node'.

-------------------------------------------------------------------------------------------------

Kibana
eyJ2ZXIiOiI4LjE0LjAiLCJhZHIiOlsiMTAuMjU1LjI1NS4yNTQ6OTIwMCJdLCJmZ3IiOiI4MTc3YmQyMGI3N2Y1ZmM2ZGM1MmViNmJhZDc3ZTczMWU0MGMwZDBiYmMyOTQxYTdiYmRlMTI5NmFmOTFhYmM0Iiwia2V5IjoiMU1ob0dKQUJjT19Va0NMaXllQUY6YnB6NEpfeS1Tc0dTQVpWS3lCNUlTQSJ9
```