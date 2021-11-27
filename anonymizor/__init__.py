import logging
import requests
import azure.functions as func
import spacy
import json
import re

def clean_ad(ad_json, nlp):
    doc = nlp(ad_json["description"])
    clean_text = ad_json["description"]

    for ent in reversed(doc.ents):
        if ent.label_ == 'PER':
            clean_text = clean_text[:ent.start_char] +ent.label_ + clean_text[ent.end_char:]

    clean_text = re.sub("[0-9 ]{5,}", " NUM ", clean_text)
    ad_json["description"] = clean_text
    return ad_json

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    req_body = req.get_json()

    TOKEN = req_body.get('secrets').get('token')
    HEADERS = {"accept": "application/json", "Authorization": f"Bearer {TOKEN}"}
    ENDPOINT = 'https://arbeidsplassen.nav.no/public-feed/api/v1/ads'

    prev_invoke = req_body.get('state').get('cursor') or '2021-11-24T16:43:45'
    prev_invoke = prev_invoke.replace('Z', '')

    prev_page = req_body.get('state').get('page')
    current_page = 0 if prev_page is None else prev_page + 1
    logging.info(prev_invoke)

    endtime = "*"
    args = f"size=100&published=%5B{prev_invoke}%2C{endtime}%5D"
    request_url = f"{ENDPOINT}?{args}&page={current_page}"

    ads_page = requests.get(request_url, headers=HEADERS)


    if ads_page.ok:
        ads_json = ads_page.json()
        ads_content = ads_json.get('content')
        nob = spacy.load('nb_core_news_sm')
        cleaned_content = [clean_ad(ad, nob) for ad in ads_content]

        
        published_times = [ad.get('published') for ad in ads_content] or [prev_invoke]
        current_highwater = req_body.get('state').get('highwater') or ''
        published_times.append(current_highwater)
        highwater_mark = max(published_times)
        has_more = not ads_json.get('last')

        if has_more:
            return_state = {
                "cursor": prev_invoke,
                "highwater": highwater_mark,
                "page": current_page
            }
        else:
            return_state = {
                "cursor": highwater_mark
            }


        return func.HttpResponse(
            json.dumps({
                "state": return_state,
                "insert": {
                    "nav_job_ads_api": cleaned_content
                },
                "schema": {
                    "transaction": {
                        "primary_key": ["uuid"]
                    }
                }, 
                "hasMore": has_more
            })
            , status_code=ads_page.status_code,  
            mimetype="application/json",
        )
    else:
        return func.HttpResponse(
             "Some error with the endpoint here, not really cool at all",
             status_code=ads_page.status_code
        )
