#!/usr/bin/env python

from invenio.search_engine import perform_request_search
from invenio.bibrank_citation_searcher import get_cited_by_count
from invenio.intbitset import intbitset
import sys
import re
import datetime
import glob
from urllib import unquote_plus
from invenio.bibrank_citation_searcher import get_cited_by
from invenio.search_engine import get_fieldvalues


verbose = True
verbose = False


#regex is a dictionary holding all the possible regular expressions that can be taken from
#the urls to search and find the associated rec id of the actual paper. rhe keys of (the regex
#dictionary are the possible regular expressions and the values are how to format them to
#retrive the appropriate paper when using the function 'perform_request_search'
arxiv_general_pattern = re.compile("arx/(mirr/)*[a-z-]+/(?P<rid>[0-9]{4}\.[0-9]+)")
arxiv_secondary_general_pattern = re.compile("arx/(mirr/)*((abs|pdf|ps)/)*(?P<rid>[a-z/0-9-]+)")
arxiv_specific_pattern = re.compile("arx/(mirr/)*((abs|pdf|ps)/)*(?P<rid>(hep|astro|nucl|gr|quant|cond)-(ph|th|qc|lat|ex|mat)/[0-9]{7})")
doi_pattern = re.compile("doi/(http://dx.doi.org/)*[0-9\.]+/(?P<rid>[a-z\.0-9/)(-]*)")
physrevlett_pattern = re.compile("doi/(http://dx.doi.org/)*(?P<rid>[0-9.]{7}/(physrevd|j.physrep|physrev|aph|ptps|epjc/s|j.nuclphysb|j.physletb|physics|physrevlett|j.astropartphys|j.nuclphysbps).[0-9./-]+)")
doi_general_pattern = re.compile("doi/(http://dx.doi.org/)*(?P<rid>[0-9)(/.-]+)")
regex = {
   # don't need these hits since we have the arXiv version...
#         arxiv_specific_pattern: (lambda match: '037:' + match.group('rid')),
#         arxiv_secondary_general_pattern: (lambda match: '037:' + match.group('rid')),
#         arxiv_general_pattern: (lambda match: '"arxiv:' + match.group('rid') + '"'),
         doi_pattern: (lambda match: (((((match.group('rid')).replace("/", " ")).replace("-", " ")).replace(".", " ")).replace(")", " ")).replace("(", " ")),
         physrevlett_pattern: (lambda match: '773:' + match.group('rid')),
         doi_general_pattern: (lambda match: match.group('rid')),
        }

blacklisted_ips = ["130.199.3.130", "128.250.54.23", "189.50.0.3",
                   "156.35.192.2", "84.237.121.107", "140.105.47.82",
                   "143.233.250.165", "147.46.56.157", "129.186.151.231",
                   "210.117.131.100", "131.220.8.9", "221.116.19.146",
                   "129.13.72.198", "130.199.3.140", "195.251.115.254",
                   "122.130.177.184", "134.107.3.147", "193.206.153.230",
                   "200.145.46.252", "149.156.47.236", "131.111.16.20",
                   "192.16.204.74", "129.104.3.5", "202.41.93.129",
                   "218.130.209.108", "194.80.32.11", "210.210.15.150",
                   "129.70.124.44", "162.105.246.189", "192.16.204.77",
                   "132.77.4.129", "132.77.4.43", "141.2.247.4",
                   "147.156.163.88", "140.105.47.84", "194.94.224.254",
                   "155.230.152.39", "129.234.4.76", "130.54.55.3",
                   "149.217.1.6", "129.2.175.71", "130.225.212.4",
                   "163.239.43.45", "210.219.50.14", "220.227.103.131",
                   "129.234.4.10", "155.230.153.238", "159.93.14.8",
                   "38.117.109.20", "202.160.174.4", "202.122.32.131"]


arx_blacklisted_ips = ["7786119ae", "d850ad5d8", "38d4282b4", "39c6080b5",
                   "b6b7ffb56", "ffe4b5dd0", "b92e643dd", "bddcba68f",
                   "15d5d8b71", "e945c4e2e", "ccc91fb83", "f1d348cde",
                   "de50a87bc", "a3772a07a", "8ff3417e0", "fc7ce2fca",
                   "fbe537527", "97176c939", "a2318f3a9", "ac63c3543",
                   "c2669d469", "a39e51e79", "1b68c255c", "42322ce17",
                   "b98217c3a", "87438682d", "957fff065", "a73454fa5",
                   "e6c5bb7d4", "9ecdb49a5", "b06795480", "0e77ca2e5",
                   "d19545f28", "cde731491", "a29e3f468", "eb55b4e93",
                   "f5642bdd1", "7ca15c422", "aff1d19c5", "59b5ac183",
                   "5131837db", "26282dfe2", "83559cbe2", "090a05ba1",
                   "8880cc50f", "4e141decd", "3c8131926", "0d61906d0",
                   "9122da39d", "09f4f0568", "be4a82014", "510192229",
                   "0be7cda06", "6fc7559c3", "dd3a9f54b", "79016361c",
                   "dee33ae1d", "20462553e", "dd64767f2", "29ac93c70",
                   "9937bf8d4", "508819e09", "4c7d472b2", "c21811ca3",
                   "2e52ef26c", "84f33c282", "bf43e524f", "c2dc51048",
                   "f1393db59", "ba255e2e1", "33a8b3417", "71343d6ff",
                   "940221264", "49d6fd26c", "3e387d192", "68e537963",
                   "d256d102f", "3057324cb", "99d376256", "cde7060cb",
                   "65c725625", "6f98e6fc4", "1219ea005", "c2f18c0f3",
                   "9bb93eed5", "a742a0896", "220e91e7d", "79e7feb99",
                   "c188cb304", "e925be648", "8ee9c8501", "918cb4642",
                   "b11d0bca4", "9fc2db8e0", "b04b53270", "2ea199674",
                   "5efaf606c", "5f1468b90", "9eb05031b", "1abf57857",
                   "0a8945bc4", "3ee3d12f6", "6d7b05166", "7b0fccc8b",
                   "f4f5c2eaf", "44c952a42", "a1eddc41e", "79b57eaeb",
                   "71cacff6c", "94bf35740", "3430b27bf", "a0f67bec0",
                   "273a495da", "e0381cc4b", "a00f177c9", "72e596fad",
                   "fd0fa78d3", "2b2753160", "6197c8b9a", "0f8b88249",
                   "939f2708c", "5b457ff9f", "ef90446b2", "5b726b25b",
                   "f4917ecf5", "e5e544d85", "3649cb004", "d2106e7ae",
                   "1be1b8f6a", "6c3546083", "663152d15", "0cc132f55",
                   "58703f541", "ecb77020d", "b19746a77", "0498a6d3e",
                   "ba1af667b", "751e303bf", "3bef910ee", "fcd677ce4",
                   "8cbeae2d4", "ff5fa1633", "d08401cd8", "3c1ac9878",
                   "d7c763504", "72a9cb58b", "61ae259d6", "8dee441f8",
                   "d5c35d7f6", "8faaaf163", "e8cbccb03", "bdca4fd52",
                   "7708eb376", "e991b49f7", "fffdb5d84", "c7932db16",
                   "af57b8f3c", "d27859b6e", "cdf3d0df8", "701409677",
                   "5da244944", "925bc3a9f", "22141d243", "326125291",
                   "c11781384", "0e1b76e6c", "299448bfa", "f99efe00c",
                   "362755b3e", "4ece74a13", "1820aef54", "c597dfdc5",
                   "2f3a3b106", "d9ce861c6", "cef7f51da", "a74afc533",
                   "0fc515ec2", "b6fc890d9", "c11f2699b", "88f5351ad",
                   "41db2b021", "c9e913cdf", "a541e74b4", "d5393c773",
                   "752cd517f", "58eb5a386", "20cf74dad", "306b26fe8",
                   "e79f74cbc", "40f0be963", "4780773e1", "ca92a297e",
                   "686d61802", "b073fa0eb", "56acd61ab", "973d40e15",
                   "dcbb0a00d", "8f21e7fa5", "c0e29e525", "8fe48ed99"]

# only matches new style arXids
arx_pattern = re.compile("\s(?P<arxid>\d{4}\.\d{4})\s")

from itertools import islice as islice

def dissect_log(n, rec_ids, date):
#   for line in islice(log_url_filter(n),0,500):
   for line in log_url_filter(n):
      rec_ids = url_count(line, rec_ids, date)
   return rec_ids


def dissect_arx_log(n, arx_ids):
#   for line in islice(log_arx_filter(n),0,100):
   for line in log_arx_filter(n):
      arx_ids = arx_count(line, arx_ids)
   return arx_ids


def log_arx_filter(n):

   ip_pattern = re.compile("(?P<ip_add>[0-9a-z]{9})\s")
   log_file = open(n, 'r')
   for line in log_file:
      if verbose:
         print line
      ip_match = ip_pattern.match(line)
      hasarx = arx_pattern.search(line)
      if ip_match and hasarx:
         if verbose:
            print  "found ip: " + ip_match.group('ip_add')
         if ip_match.group('ip_add') not in arx_blacklisted_ips:
            yield hasarx.group('arxid')
   log_file.close()

def log_url_filter(n):
   log_file = open(n, 'r')
   url_pattern = re.compile("Pid\s[0-9]+\s(?P<ip_add>[0-9.]+)\s.*outgoing/[0-9a-zA-Z\.:\-/]*")

   for line in log_file:
      line = unquote_plus(line)
      url_match = url_pattern.search(line)
      if url_match:
         if url_match.group('ip_add') not in blacklisted_ips:
            yield line
   log_file.close()


def arx_count(line, arx_ids):
   arxid = line
   if verbose:
      print arxid + "found "
   if arxid not in arx_ids:
      arx_ids[arxid] = 1
      if verbose:
         print arxid + "new "
   else:
      arx_ids[arxid] += 1
      if verbose:
         print arxid + "+1 "
   return(arx_ids)


def arx_deref(rec_ids, arx_ids, date):
   for (arxid, count) in arx_ids.iteritems():
      search_results = perform_request_search(p="find eprint arxiv:"+arxid)
      if len(search_results) == 1:
         if dates_pass(search_results[0], date):
            if verbose:
               print arxid + "date "
            if search_results[0] not in rec_ids:
               rec_ids[search_results[0]] = count
               if verbose:
                  print arxid + "new "
            else:
               rec_ids[search_results[0]] += count
               if verbose:
                  print arxid + "+1 "
   return rec_ids

def url_count(url_line, rec_ids, date):
   pattern_found = False

   for pattern in regex:
      result = pattern.search(url_line)
      if result and pattern_found == False:
         search_results = perform_request_search(p=regex[pattern](result))
         if len(search_results) == 1:
            pattern_found = True
            if dates_pass(search_results[0], date):
               if search_results[0] not in rec_ids:
                  rec_ids[search_results[0]] = 1
               else:
                  rec_ids[search_results[0]] += 1

   #display the url if no pattern in the regex dictionary matched it
#   if pattern_found == False:
#      sys.stderr.write("NO MATCH: " + url_line + "\n")
   return rec_ids

def dates_pass(rid, date):
   MONTH_OFFSET_TWO = datetime.timedelta(60)
   MONTH_OFFSET_ONE = datetime.timedelta(30)
   first_month = date - MONTH_OFFSET_TWO
   second_month = date - MONTH_OFFSET_ONE
   filter_list = (first_month.strftime("%Y-%m"),second_month.strftime("%Y-%m"))
   rid_fieldvalues = get_fieldvalues(rid, '269__c')
   if len(rid_fieldvalues) == 1:
      return rid_fieldvalues[0] in filter_list
   return False


def print_rec_ids(rec_ids,offset=365):


   print "Rec ID, Clicks,date, arXiv, Citations(1yr), Citations(6mo):"
   output = []
   for key in rec_ids:
      dates = get_fieldvalues(key, '269__c')
      if len(dates) > 0:
         date = dates[0]
      reps = get_fieldvalues(key, '037__a')
      if len(reps) > 0:
         rep = reps[0]
      cats = get_fieldvalues(key, '037__c')
      if len(cats) > 0:
         cat = cats[0]
      output.append([key, rec_ids[key], date, rep, cat])
   date1=''
   output.sort(key = lambda record:record[2])
   for record in output:
      if record[2] != date1:
         date = datetime.date(int(record[2].rsplit('-')[0]),int(record[2].rsplit('-')[1]),1)
         date2 = date + datetime.timedelta(offset/2)
         date3 = date + datetime.timedelta(offset)
         ## check and split across yearsdue to search bug.   assumes that
         ## if small offset splits the year, the big one does too (i.e. we
         ## don't go back or forward more than 6 mos
         if date.year != date2.year:
            join = str(date.year) +'-12-31 or year:' + str(date2.year) + '-01-01->'
         else:
            join = ''
         date1 = date.strftime("%Y-%m")
         date2 = date2.strftime("%Y-%m")
         date3 = date3.strftime("%Y-%m")

         print date1, date2, date3
         complete_paper_list = intbitset(perform_request_search(p='year:'+date1+'->' + join + date2))
         half_complete_paper_list = intbitset(perform_request_search(p='year:'+date1+'->' + join + date3))
      paper_citation_list = intbitset(get_cited_by(record[0]))
      narrowed_citation_count = len(paper_citation_list & complete_paper_list)
      half_narrowed_citation_count = len(paper_citation_list & half_complete_paper_list)

      print '%d,%d,%s,%s,%s,%d,%d' % (record[0],record[1],record[2],record[3],record[4], half_narrowed_citation_count,narrowed_citation_count)

def main():
   rec_ids = {}
   arx_ids = {}
   logs_dir = "/home/tcb/logs/"

   datestart = datetime.date(2009,1,1)
   names = ["Null","Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul"]

   for month in range(1,7):

      date = datestart.replace(month=month)
      fileglob = logs_dir + "spiface.??-" + names[month] + "-2009.log"
      for filename in glob.glob(fileglob):
         if verbose:
            print "reading "+ filename
         rec_ids = dissect_log(filename, rec_ids, date)
      # Now do arXiv
      fileglob = logs_dir + "arXiv/access_log.090" + str(month) + "??_iphash_id_date"
      for filename in glob.glob(fileglob):
         if verbose:
            print "reading "+ filename
         arx_ids = dissect_arx_log(filename, arx_ids)
      if len(arx_ids) > 0:
         if verbose:
            print "dereferencing arx IDs from "+ filename
         rec_ids = arx_deref(rec_ids, arx_ids, date)

   print_rec_ids(rec_ids)


if __name__ == "__main__":
   main()
