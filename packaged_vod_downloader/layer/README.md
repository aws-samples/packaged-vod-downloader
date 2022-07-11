# Rebuilting Lambda layer

This project includes a zip file to create the lambda layer for the project.

The lambda layer includes all the external modules required by the Lambda Function.

The modules built into the layer are listed in the requirements.txt file.

If the project is modified and the lambda function requires additional dependencies the zip file can be recreated by following the steps below.

The first step is to list the additional modules to be included in the project in the requirements.txt.

These steps start from the root directory of the project.

```
cd packaged_vod_downloader/layer
mkdir python
pip install -r requirements.txt -t python
zip -r layer_package.zip python
```
