# S3 Assets Toolkit

small tools, designed to be combined together or with other tools (Unix
principle)

## Rollback S3 bucket in-place to some point of time

If you activated versioning for your S3 bucket (TODO add link), ...

## Set Cache-Control for every object in the bucket

Unfortunately AWS provides no global setting, neither for its CloudFront
CDN, nor for S3, to activate browser caching (`Cache-Control` header)
globally.

So whenever uploading every single object to the bucket, think about
setting `--cache-control`. But what if you already have got millions of
objects in your bucket and need to activate browser caching for them?

`put-cache-control` to the rescue!

## Extract top requested objects from CloudFront CSV report

`extract-top-from-csv.rb`


