import os
import re
import json
import time
import arxiv
import yaml
import logging
import argparse
import datetime
import requests
from typing import Optional

logging.basicConfig(format='[%(asctime)s %(levelname)s] %(message)s',
                    datefmt='%m/%d/%Y %H:%M:%S',
                    level=logging.INFO)

base_url = "https://huggingface.co/api/papers/"
github_url = "https://api.github.com/search/repositories"
arxiv_url = "http://arxiv.org/"

def load_config(config_file:str) -> dict:
    '''
    config_file: input config file path
    return: a dict of configuration
    '''
    # make filters pretty
    def pretty_filters(**config) -> dict:
        keywords = dict()
        EXCAPE = '\"'
        QUOTA = '' # NO-USE
        OR = 'OR' # TODO
        def parse_filters(filters:list):
            ret = ''
            for idx in range(0,len(filters)):
                filter = filters[idx]
                if len(filter.split()) > 1:
                    ret += (EXCAPE + filter + EXCAPE)  
                else:
                    ret += (QUOTA + filter + QUOTA)   
                if idx != len(filters) - 1:
                    ret += OR
            return ret
        for k,v in config['keywords'].items():
            keywords[k] = parse_filters(v['filters'])
        return keywords
    with open(config_file,'r') as f:
        config = yaml.load(f,Loader=yaml.FullLoader) 
        config['kv'] = pretty_filters(**config)
        logging.info(f'config = {config}')
    return config 

def get_authors(authors, first_author = False):
    output = str()
    if first_author == False:
        output = ", ".join(str(author) for author in authors)
    else:
        output = authors[0]
    return output
def sort_papers(papers):
    output = dict()
    keys = list(papers.keys())
    keys.sort(reverse=True)
    for key in keys:
        output[key] = papers[key]
    return output

# Semantic Scholar 公开 API 说明：https://api.semanticscholar.org/api-docs/
# 无 API key 时官方约 100 次/5 分钟；两次请求之间需留出足够间隔，否则会 429。
S2_GRAPH_PAPER_URL = "https://api.semanticscholar.org/graph/v1/paper/arXiv:{arxiv_id}"
_S2_LAST_REQUEST_END = 0.0


def _s2_min_interval_seconds() -> float:
    return 0.4 if os.environ.get("SEMANTIC_SCHOLAR_API_KEY") else 3.15


def _semantic_scholar_throttle_before_request() -> None:
    """在发起 S2 请求前节流，降低无 key 时的 429 概率。"""
    global _S2_LAST_REQUEST_END
    interval = _s2_min_interval_seconds()
    now = time.time()
    wait = _S2_LAST_REQUEST_END + interval - now
    if wait > 0:
        time.sleep(wait)


def _semantic_scholar_mark_request_done() -> None:
    global _S2_LAST_REQUEST_END
    _S2_LAST_REQUEST_END = time.time()


def _retry_after_seconds(response: requests.Response) -> Optional[int]:
    """解析 Retry-After（秒）；无法解析时返回 None。"""
    ra = response.headers.get("Retry-After")
    if ra is None:
        return None
    try:
        return int(ra.strip()) + 1
    except ValueError:
        return None


def fetch_semantic_scholar_citation_count(arxiv_id: str) -> Optional[int]:
    """
    根据 arXiv ID 查询 Semantic Scholar 上的被引次数。
    新稿或未收录时可能返回 None。
    """
    aid = (arxiv_id or "").strip()
    if not aid:
        return None
    url = S2_GRAPH_PAPER_URL.format(arxiv_id=aid)
    params = {"fields": "citationCount"}
    headers = {
        "User-Agent": "ASR-TTS-paper-daily/1.0 (citation lookup; contact: repo maintainer)",
    }
    api_key = os.environ.get("SEMANTIC_SCHOLAR_API_KEY")
    if api_key:
        headers["x-api-key"] = api_key
    max_attempts = 5
    try:
        last_err = None
        for attempt in range(max_attempts):
            _semantic_scholar_throttle_before_request()
            r = requests.get(url, params=params, headers=headers, timeout=20)
            _semantic_scholar_mark_request_done()
            if r.status_code == 429:
                ra = _retry_after_seconds(r)
                # 无 Retry-After 时用较长退避，避免连续撞限流
                wait = ra if ra is not None else min(120, 20 + 25 * attempt)
                last_err = (r.text or "")[:200]
                logging.warning(
                    "Semantic Scholar 限流(429)，%s 秒后重试 (%s/%s) arxiv=%s",
                    wait,
                    attempt + 1,
                    max_attempts,
                    aid,
                )
                time.sleep(wait)
                continue
            if r.status_code == 404:
                return None
            r.raise_for_status()
            data = r.json()
            if "citationCount" not in data:
                return None
            return int(data["citationCount"])
        logging.warning("Semantic Scholar 多次限流后放弃 arxiv=%s: %s", aid, last_err)
        return None
    except Exception as e:
        logging.warning("Semantic Scholar 查询失败 arxiv=%s: %s", aid, e)
        _semantic_scholar_mark_request_done()
        return None


def get_code_link(qword:str) -> str:
    """
    This short function was auto-generated by ChatGPT. 
    I only renamed some params and added some comments.
    @param qword: query string, eg. arxiv ids and paper titles
    @return paper_code in github: string, if not found, return None
    """
    # query = f"arxiv:{arxiv_id}"
    query = f"{qword}"
    params = {
        "q": query,
        "sort": "stars",
        "order": "desc"
    }
    r = requests.get(github_url, params=params)
    results = r.json()
    code_link = None
    if results["total_count"] > 0:
        code_link = results["items"][0]["html_url"]
    return code_link
  
def get_daily_papers(
    topic,
    query="slam",
    max_results=2,
    fetch_citations=True,
    min_citations=0,
):
    """
    @param topic: str
    @param query: str
    @param fetch_citations: 是否查询 Semantic Scholar 被引次数（便于筛文）
    @param min_citations: 仅保留被引次数 >= 该值的论文；0 表示不过滤。需「多于 10 次」时填 11。
    @return paper_with_code: dict
    """
    # output
    content = dict()
    content_to_web = dict()
    try:
        min_citations = int(min_citations)
    except (TypeError, ValueError):
        min_citations = 0
    if min_citations < 0:
        min_citations = 0
    if min_citations > 0:
        logging.info(
            "仅保留 Semantic Scholar 被引次数 >= %s 的论文（无记录或低于阈值将跳过）",
            min_citations,
        )
    effective_fetch = fetch_citations or (min_citations > 0)
    search_engine = arxiv.Search(
        query = query,
        max_results = max_results,
        sort_by = arxiv.SortCriterion.SubmittedDate
    )

    for result in search_engine.results():

        paper_id            = result.get_short_id()
        paper_title         = result.title
        paper_url           = result.entry_id
        # code_url            = base_url + paper_id #TODO
        paper_abstract      = result.summary.replace("\n"," ")
        paper_authors       = get_authors(result.authors)
        paper_first_author  = get_authors(result.authors,first_author = True)
        primary_category    = result.primary_category
        publish_time        = result.published.date()
        update_time         = result.updated.date()
        comments            = result.comment

        logging.info(f"Time = {update_time} title = {paper_title} author = {paper_first_author}")

        # eg: 2108.09112v1 -> 2108.09112
        ver_pos = paper_id.find('v')
        if ver_pos == -1:
            paper_key = paper_id
        else:
            paper_key = paper_id[0:ver_pos]    
        paper_url = arxiv_url + 'abs/' + paper_key
        code_url = base_url + paper_key

        cite_str = "—"
        cite_n = None
        if effective_fetch:
            cite_n = fetch_semantic_scholar_citation_count(paper_key)
            if cite_n is not None:
                s2_q = f"https://www.semanticscholar.org/search?q=arXiv%3A{paper_key}&sort=citation-count"
                cite_str = f"[{cite_n}]({s2_q})"
            # S2 请求间隔由 fetch_semantic_scholar_citation_count 内全局节流控制

        if min_citations > 0:
            if cite_n is None or cite_n < min_citations:
                logging.info(
                    "跳过（被引 %s < min_citations=%s）: %s — %s",
                    cite_n if cite_n is not None else "无",
                    min_citations,
                    paper_key,
                    paper_title[:80],
                )
                continue

        try:
            # source code link    
            r = requests.get(code_url).json()
            repo_url = None
            if "githubRepo" in r:
                repo_url = r["githubRepo"]

            # TODO: not found, two more chances  
            # else: 
            #    repo_url = get_code_link(paper_title)
            #    if repo_url is None:
            #        repo_url = get_code_link(paper_key)
            if repo_url is not None:
                content[paper_key] = (
                    "|**{}**|**{}**|{} et.al.|{}|[{}]({})|**[link]({})**|\n".format(
                        update_time,
                        paper_title,
                        paper_first_author,
                        cite_str,
                        paper_key,
                        paper_url,
                        repo_url,
                    )
                )
                content_to_web[paper_key] = (
                    "- {}, **{}**, {} et.al., 被引: {}, Paper: [{}]({}), Code: **[{}]({})**".format(
                        update_time,
                        paper_title,
                        paper_first_author,
                        cite_str,
                        paper_url,
                        paper_url,
                        repo_url,
                        repo_url,
                    )
                )

            else:
                content[paper_key] = (
                    "|**{}**|**{}**|{} et.al.|{}|[{}]({})|null|\n".format(
                        update_time,
                        paper_title,
                        paper_first_author,
                        cite_str,
                        paper_key,
                        paper_url,
                    )
                )
                content_to_web[paper_key] = (
                    "- {}, **{}**, {} et.al., 被引: {}, Paper: [{}]({})".format(
                        update_time,
                        paper_title,
                        paper_first_author,
                        cite_str,
                        paper_url,
                        paper_url,
                    )
                )

            # TODO: select useful comments
            comments = None
            if comments != None:
                content_to_web[paper_key] += f", {comments}\n"
            else:
                content_to_web[paper_key] += f"\n"

        except Exception as e:
            logging.error(f"exception: {e} with id: {paper_key}")

    data = {topic:content}
    data_web = {topic:content_to_web}
    return data,data_web 

def update_paper_links(filename):
    '''
    weekly update paper links in json file 
    '''
    def parse_arxiv_string(s):
        """解析表格行；兼容无 Citations 列的旧数据（旧行 split 后长度为 6）。"""
        parts = [p.strip() for p in s.split("|")]
        if len(parts) < 6:
            return "", "", "", "—", "", "null"
        date, title, authors = parts[1], parts[2], parts[3]
        # 新格式多一列：|…|Authors|Citations|PDF|Code|
        if len(parts) >= 7:
            citations = parts[4]
            pdf_md = parts[5]
            code = parts[6] if len(parts) > 6 else "null"
        else:
            citations = "—"
            pdf_md = parts[4]
            code = parts[5] if len(parts) > 5 else "null"
        pdf_md = re.sub(r"v\d+", "", pdf_md)
        return date, title, authors, citations, pdf_md, code

    with open(filename,"r") as f:
        content = f.read()
        if not content:
            m = {}
        else:
            m = json.loads(content)
            
        json_data = m.copy() 

        for keywords,v in json_data.items():
            logging.info(f'keywords = {keywords}')
            for paper_id,contents in v.items():
                contents = str(contents)

                update_time, paper_title, paper_first_author, citations, pdf_md, code_url = parse_arxiv_string(
                    contents
                )

                contents = "|{}|{}|{}|{}|{}|{}|\n".format(
                    update_time,
                    paper_title,
                    paper_first_author,
                    citations,
                    pdf_md,
                    code_url,
                )
                json_data[keywords][paper_id] = str(contents)
                logging.info(f'paper_id = {paper_id}, contents = {contents}')
                
                valid_link = False if "|null|" in contents else True
                if valid_link:
                    continue
                try:
                    code_url = base_url + paper_id #TODO
                    r = requests.get(code_url).json()
                    repo_url = None
                    if "official" in r and r["official"]:
                        repo_url = r["official"]["url"]
                        if repo_url is not None:
                            new_cont = contents.replace('|null|',f'|**[link]({repo_url})**|')
                            logging.info(f'ID = {paper_id}, contents = {new_cont}')
                            json_data[keywords][paper_id] = str(new_cont)

                except Exception as e:
                    logging.error(f"exception: {e} with id: {paper_id}")
        # dump to json file
        with open(filename,"w") as f:
            json.dump(json_data,f)

def update_json_file(filename,data_dict):
    '''
    daily update json file using data_dict
    '''
    with open(filename,"r") as f:
        content = f.read()
        if not content:
            m = {}
        else:
            m = json.loads(content)
            
    json_data = m.copy() 
    
    # update papers in each keywords         
    for data in data_dict:
        for keyword in data.keys():
            papers = data[keyword]

            if keyword in json_data.keys():
                json_data[keyword].update(papers)
            else:
                json_data[keyword] = papers

    with open(filename,"w") as f:
        json.dump(json_data,f)
    
def json_to_md(filename,md_filename,
               task = '',
               to_web = False, 
               use_title = True, 
               use_tc = True,
               show_badge = True,
               use_b2t = True):
    """
    @param filename: str
    @param md_filename: str
    @return None
    """
    def pretty_math(s:str) -> str:
        ret = ''
        match = re.search(r"\$.*\$", s)
        if match == None:
            return s
        math_start,math_end = match.span()
        space_trail = space_leading = ''
        if s[:math_start][-1] != ' ' and '*' != s[:math_start][-1]: space_trail = ' ' 
        if s[math_end:][0] != ' ' and '*' != s[math_end:][0]: space_leading = ' ' 
        ret += s[:math_start] 
        ret += f'{space_trail}${match.group()[1:-1].strip()}${space_leading}' 
        ret += s[math_end:]
        return ret
  
    DateNow = datetime.date.today()
    DateNow = str(DateNow)
    DateNow = DateNow.replace('-','.')
    
    with open(filename,"r") as f:
        content = f.read()
        if not content:
            data = {}
        else:
            data = json.loads(content)

    # clean README.md if daily already exist else create it
    with open(md_filename,"w+") as f:
        pass

    # write data into README.md
    with open(md_filename,"a+") as f:

        if (use_title == True) and (to_web == True):
            f.write("---\n" + "layout: default\n" + "---\n\n")
        
        if show_badge == True:
            f.write(f"[![Contributors][contributors-shield]][contributors-url]\n")
            f.write(f"[![Forks][forks-shield]][forks-url]\n")
            f.write(f"[![Stargazers][stars-shield]][stars-url]\n")
            f.write(f"[![Issues][issues-shield]][issues-url]\n\n")    
                
        if use_title == True:
            #f.write(("<p align="center"><h1 align="center"><br><ins>CV-ARXIV-DAILY"
            #         "</ins><br>Automatically Update CV Papers Daily</h1></p>\n"))
            f.write("## Updated on " + DateNow + "\n")
        else:
            f.write("> Updated on " + DateNow + "\n")

        # TODO: add usage
        f.write("> Usage instructions: [here](./docs/README.md#usage)\n\n")
        f.write("> This page is modified from [here](https://github.com/Vincentqyw/cv-arxiv-daily)\n\n")

        #Add: table of contents
        if use_tc == True:
            f.write("<details>\n")
            f.write("  <summary>Table of Contents</summary>\n")
            f.write("  <ol>\n")
            for keyword in data.keys():
                day_content = data[keyword]
                if not day_content:
                    continue
                kw = keyword.replace(' ','-')      
                f.write(f"    <li><a href=#{kw.lower()}>{keyword}</a></li>\n")
            f.write("  </ol>\n")
            f.write("</details>\n\n")
        
        for keyword in data.keys():
            day_content = data[keyword]
            if not day_content:
                continue
            # the head of each part
            f.write(f"## {keyword}\n\n")

            if use_title == True :
                if to_web == False:
                    f.write(
                        "|Publish Date|Title|Authors|Citations|PDF|Code|\n"
                        + "|---|---|---|---|---|---|\n"
                    )
                else:
                    f.write(
                        "| Publish Date | Title | Authors | Citations | PDF | Code |\n"
                    )
                    f.write(
                        "|:---------|:-----------------------|:---------|:---------|:------|:------|\n"
                    )

            # sort papers by date
            day_content = sort_papers(day_content)
        
            for _,v in day_content.items():
                if v is not None:
                    f.write(pretty_math(v)) # make latex pretty

            f.write(f"\n")
            
            #Add: back to top
            if use_b2t:
                top_info = f"#Updated on {DateNow}"
                top_info = top_info.replace(' ','-').replace('.','')
                f.write(f"<p align=right>(<a href={top_info.lower()}>back to top</a>)</p>\n\n")
            
        if show_badge == True:
            # we don't like long string, break it!
            f.write((f"[contributors-shield]: https://img.shields.io/github/"
                     f"contributors/Vincentqyw/cv-arxiv-daily.svg?style=for-the-badge\n"))
            f.write((f"[contributors-url]: https://github.com/Vincentqyw/"
                     f"cv-arxiv-daily/graphs/contributors\n"))
            f.write((f"[forks-shield]: https://img.shields.io/github/forks/Vincentqyw/"
                     f"cv-arxiv-daily.svg?style=for-the-badge\n"))
            f.write((f"[forks-url]: https://github.com/Vincentqyw/"
                     f"cv-arxiv-daily/network/members\n"))
            f.write((f"[stars-shield]: https://img.shields.io/github/stars/Vincentqyw/"
                     f"cv-arxiv-daily.svg?style=for-the-badge\n"))
            f.write((f"[stars-url]: https://github.com/Vincentqyw/"
                     f"cv-arxiv-daily/stargazers\n"))
            f.write((f"[issues-shield]: https://img.shields.io/github/issues/Vincentqyw/"
                     f"cv-arxiv-daily.svg?style=for-the-badge\n"))
            f.write((f"[issues-url]: https://github.com/Vincentqyw/"
                     f"cv-arxiv-daily/issues\n\n"))
                
    logging.info(f"{task} finished")        

def demo(**config):
    # TODO: use config
    data_collector = []
    data_collector_web= []
    
    keywords = config['kv']
    max_results = config['max_results']
    publish_readme = config['publish_readme']
    publish_gitpage = config['publish_gitpage']
    publish_wechat = config['publish_wechat']
    show_badge = config['show_badge']

    b_update = config['update_paper_links']
    logging.info(f'Update Paper Link = {b_update}')
    if config['update_paper_links'] == False:
        logging.info(f"GET daily papers begin")
        for topic, keyword in keywords.items():
            logging.info(f"Keyword: {topic}")
            data, data_web = get_daily_papers(
                topic,
                query=keyword,
                max_results=max_results,
                fetch_citations=config.get("fetch_citations", True),
                min_citations=int(config.get("min_citations") or 0),
            )
            data_collector.append(data)
            data_collector_web.append(data_web)
            print("\n")
        logging.info(f"GET daily papers end")

    # 1. update README.md file
    if publish_readme:
        json_file = config['json_readme_path']
        md_file   = config['md_readme_path']
        # update paper links
        if config['update_paper_links']:
            update_paper_links(json_file)
        else:    
            # update json data
            update_json_file(json_file,data_collector)
        # json data to markdown
        json_to_md(json_file,md_file, task ='Update Readme', \
            show_badge = show_badge)

    # 2. update docs/index.md file (to gitpage)
    if publish_gitpage:
        json_file = config['json_gitpage_path']
        md_file   = config['md_gitpage_path']
        # TODO: duplicated update paper links!!!
        if config['update_paper_links']:
            update_paper_links(json_file)
        else:    
            update_json_file(json_file,data_collector)
        json_to_md(json_file, md_file, task ='Update GitPage', \
            to_web = True, show_badge = show_badge, \
            use_tc=False, use_b2t=False)

    # 3. Update docs/wechat.md file
    if publish_wechat:
        json_file = config['json_wechat_path']
        md_file   = config['md_wechat_path']
        # TODO: duplicated update paper links!!!
        if config['update_paper_links']:
            update_paper_links(json_file)
        else:    
            update_json_file(json_file, data_collector_web)
        json_to_md(json_file, md_file, task ='Update Wechat', \
            to_web=False, use_title= False, show_badge = show_badge)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--config_path',type=str, default='config.yaml',
                            help='configuration file path')
    parser.add_argument('--update_paper_links', default=False,
                        action="store_true",help='whether to update paper links etc.')                        
    args = parser.parse_args()
    config = load_config(args.config_path)
    config = {**config, 'update_paper_links':args.update_paper_links}
    demo(**config)
