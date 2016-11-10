#!/usr/bin/env ruby
# encoding: UTF-8

# Extract file names from AWS Console > CloudFront > Popular Object > CSV report
# Note: please download this CSV reports to statistics folder first
res = []
Dir.glob("statistics/*.csv").each do |f|
  IO.read(f).each_line do |l|
    if l =~ /"(\/[^"]+)"/
      res << $1[1..-1]
    end
  end
end
puts res.sort.uniq
