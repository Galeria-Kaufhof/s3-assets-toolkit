#!/usr/bin/env ruby
# encoding: UTF-8
require 'json'

# Reads a plain list of object(file) names from stdin
# and ensures desired Cache-Control setting on them
# Example usage: $0 bucket-name 172800,public
# (ensure caching for 2 days)
fail "Please provide bucket name and desired Cache-Control setting" if ARGV.size != 2
bucket = ARGV[0]
newvalue = ARGV[1]

$stdin.each_line do |l|
  name = l.strip
  meta = `aws s3api head-object --bucket #{bucket} --key '#{name}'`
  if meta.size == 0
    print '-' # not found, object already gone
    next
  end
  d = JSON.parse(meta)
  type = d["ContentType"]
  cache = d["CacheControl"]
  # . - skip
  # X - type was not set, set to image/png
  # j - was image/jpeg; adjusted CacheControl
  # g - was image/png; adjusted CacheControl
  # P - pdf file; adjusted CacheControl
  # Y - other file type; adjusted CacheControl
  if cache == newvalue
    status = '.'
  else
    puts "\n#{name} - will set to  #{type} #{newvalue}"
    if type.nil?
      status = 'X'
      type = 'image/png'
    elsif type == 'image/png'
      status = 'g'
    elsif type == 'image/jpeg'
      status = 'j'
    elsif type == 'application/pdf'
      status = 'P'
    else
      status = 'Y'
    end
    cmd = "aws s3 cp --metadata-directive REPLACE --cache-control '#{newvalue}' s3://#{bucket}/#{name} s3://#{bucket}/#{name} --content-type '#{type}'"
    unless system(cmd)
      puts "\nFailed command:", cmd
    end
  end
  print status
end

