import os
import json
import datetime
import pytz
import requests
import re
import html
import hmac
import hashlib
import boto3
import psycopg2
import difflib

post_url = 'https://slack.com/api/chat.postMessage'
post_eph_url = 'https://slack.com/api/chat.postEphemeral'
user_info_url = 'https://slack.com/api/users.info'

half_thresh = 0.5
full_thresh = 0.8

subscriber_list = dict()
subscriber_list['+19176568387'] = 'Luke Kavenagh'
subscriber_list['+19166281782'] = 'Barbara Evangelista'
subscriber_list['+17184501263'] = 'John Cintolo'

def dbGetQuery(q):
    conn_string = "host='kavdb.c9lrodma91yx.us-west-2.rds.amazonaws.com' dbname='kavdb' user='lkavenagh' password='" + os.environ['pw'] + "'"
    try:
        conn = psycopg2.connect(conn_string)
    except:
        print('Connection to AWS failed, waiting...')
        time.sleep(2)
        print('Retrying...')
        conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    
    cursor = conn.cursor()
    cursor.execute(q)
    dat = cursor.fetchall()
    
    return(dat)
    
def dbSendQuery(q):
    conn_string = "host='kavdb.c9lrodma91yx.us-west-2.rds.amazonaws.com' dbname='kavdb' user='lkavenagh' password='" + os.environ['pw'] + "'"
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    cursor = conn.cursor()
    try:
        cursor.execute(q)
        cursor.close()
        conn.close()
    except:
        cursor.close()
        conn.close()
        raise
    
def prev_weekday(adate):
    adate -= datetime.timedelta(days=1)
    while adate.weekday() > 4: # Mon-Fri are 0-4
        adate -= datetime.timedelta(days=1)
    return adate

def post_message_to_channel(msg, channel, attachments = None):
    headers = {'Content-type':'application/json', 
               'Authorization':'Bearer {}'.format(os.environ['jeopardybottoken']),
              }
    payload = {
        'channel': channel,
        'text': msg,
        'username': 'jeopardybot',
        'icon_url': 'https://images.app.goo.gl/vfVaBTWhP9RsV53u8',
        'attachments': attachments
    }
    requests.post(post_url, data=json.dumps(payload), headers = headers)
    
def send_sms(to, msg):
    region = "us-west-2"

    originationNumber = os.environ['pinpoint_longcode']
    destinationNumber = to
    
    message = msg
    
    applicationId = os.environ['pinpoint_app_id']
    
    messageType = "TRANSACTIONAL"
    registeredKeyword = 'keyword_004092790924'
    
    senderId = 'jeopardy'
    
    client = boto3.client('pinpoint',region_name=region)
    
    response = client.send_messages(
        ApplicationId=applicationId,
        MessageRequest={
            'Addresses': {
                destinationNumber: {
                    'ChannelType': 'SMS'
                }
            },
            'MessageConfiguration': {
                'SMSMessage': {
                    'Body': message,
                    'Keyword': registeredKeyword,
                    'MessageType': messageType,
                    'OriginationNumber': originationNumber,
                    'SenderId': senderId
                }
            }
        }
    )
    
def send_sms_to_all(msg):
    for dest in subscriber_list.keys():
        send_sms(dest, msg)
    
def post_ephemeral_message_to_user_in_channel(msg, channel, user_id = os.environ['slacklukeid']):
    headers = {'Content-type':'application/json', 
               'Authorization':'Bearer {}'.format(os.environ['jeopardybottoken']),
              }
    payload = {
        'channel': channel,
        'text': msg,
        'user': user_id
    }
    requests.post(post_eph_url, data=json.dumps(payload), headers = headers)
    
def get_user_info(user_id):
    if user_id[0] == '+':
        #It's a phone number, check the subscriber list

        user = dict()
        user['user'] = dict()
        
        user['user']['real_name'] = subscriber_list[user_id]

        return(user)
    else:
        payload = '{}?token={}&user={}'.format(user_info_url, os.environ['jeopardybottoken'], user_id)
        r = requests.get(payload)
        return(r.json())
    
def record_guess(user_id, jeopardy_date, response_date, response, oc, is_sms = False):
    name = get_user_info(user_id)['user']['real_name']
    if is_sms:
        post_msg = 'Recording {}\'s response (via SMS) for the {} final jeopardy as "{}"'.format(name, jeopardy_date.strftime('%b %-d, %Y'), response)
    else:
        post_msg = 'Recording {}\'s response for the {} final jeopardy as "{}"'.format(name, jeopardy_date.strftime('%b %-d, %Y'), response)
    
    post_message_to_channel(post_msg, oc)
    
    if is_sms:
        send_sms(user_id, post_msg)
    
    q = """INSERT INTO jeopardy.guesses (name, jeopardy_date, response_time, response)
            VALUES ('{name}', '{jd}', '{rd}', '{r}')
        """.format(name = name, jd = jeopardy_date, rd = response_date, r = response)
    
    dbSendQuery(q)
    
def record_publish(jeopardy_date, qa, full_text, post_time):
    q = """INSERT INTO jeopardy.publish (jeopardy_date, question_or_answer, full_text, post_time) 
            VALUES ('{jd}', '{qa}', '{ft}', '{pt}')
        """.format(jd = jeopardy_date, qa = qa, ft = full_text, pt = str(post_time))
    dbSendQuery(q)
    
def similarity(a,b):
    response = a.lower()
    response = re.sub('-', ' ', response)
    response = re.sub('[^a-z0-9 ]', '', response)
    response = re.sub('^w.*?(is|are) ', '', response)

    answer = b.lower()
    answer = re.sub('-', ' ', answer)
    answer = re.sub('[^a-z0-9 ]', '', answer)
    answer = re.sub('^w.*?(is|are) ', '', answer)
    
    return difflib.SequenceMatcher(None, response, answer).ratio()

def get_jeopardy_data(dt):
    try:
        jeopardy_data = dict()
        question_url = 'https://thejeopardyfan.com/{}/{}/final-jeopardy-{}-{}-{}.html'.format(dt.strftime('%Y'), dt.strftime('%m'), dt.strftime('%-m'), dt.strftime('%-d'), dt.strftime('%Y'))
        full_text = requests.get(question_url).text.replace('\n', '_')
        print(full_text)
        
        category = html.unescape(re.search('\(in the category <strong>(.*?)</strong>', full_text).group(1))
        q = html.unescape(re.search('{}.*?red[;]*">(<strong>)*(.*?)<'.format(dt.strftime('%A, %B %-d, %Y')), full_text).group(2))
        read_dt = re.search('Today.s Final Jeopardy answer.*?and (statistics|stats) for (?:the )*(.*?[0-9]{4})', full_text).group(2)
        read_dt = datetime.datetime.strptime(read_dt, '%A, %B %d, %Y')
        
        a = html.unescape(re.search('Correct response:.*?red[;]*">(.*?)</[span|strong]', full_text).group(1))
        extended_a = html.unescape(re.search('More information about Final Jeopardy:.*?>([A-Za-z].*?)</p>', full_text).group(1))
        
        jeopardy_data['question_raw'] = q
        jeopardy_data['question'] = 'Final Jeopardy: {:%A, %b %#d}\n\nCategory: {}\n\n{}'.format(read_dt, category, q)
        jeopardy_data['answer'] = re.sub('<.*?>', '', a)
        jeopardy_data['extended_answer'] = re.sub('<.*?>', '', extended_a)
    except Exception as e:
        jeopardy_data = dict()
        jeopardy_data['question_raw'] = None
        jeopardy_data['question'] = None
        jeopardy_data['answer'] = None
        print(e)
        
    
    return(jeopardy_data)
    
def get_leaderboard_data(period = 'month', ex_bandwagon = False):
    if period == 'month':
        q = """
            WITH results AS (
              SELECT a.jeopardy_date, a.name, a.response, a.response_time, 
                  min(case when b.question_or_answer = 'question' then full_text end) as question,
                  min(case when b.question_or_answer = 'answer' then full_text end) as answer,
                  min(case when b.question_or_answer = 'question' then post_time end) as question_post_time,
                  min(case when b.question_or_answer = 'answer' then post_time end) as answer_post_time,
                  response_time = min(response_time) over (partition by a.jeopardy_date, a.response) as first_responder
                FROM jeopardy.guesses a
                JOIN jeopardy.publish b
                ON a.jeopardy_date = b.jeopardy_date
                GROUP BY 1,2,3,4
            )
            
            SELECT a.*, b.name
            FROM results a
            JOIN (SELECT * FROM results WHERE first_responder = true) b
            ON a.jeopardy_date = b.jeopardy_date
            AND a.response = b.response
            WHERE a.response_time < a.answer_post_time
            AND EXTRACT(month from a.response_time) = EXTRACT(month from current_date)
            AND EXTRACT(year from a.response_time) = EXTRACT(year from current_date)
            ORDER BY a.jeopardy_date, a.response_time, a.name
            """
    elif period == 'lastmonth':
        q = """
            WITH results AS (
              SELECT a.jeopardy_date, a.name, a.response, a.response_time, 
                  min(case when b.question_or_answer = 'question' then full_text end) as question,
                  min(case when b.question_or_answer = 'answer' then full_text end) as answer,
                  min(case when b.question_or_answer = 'question' then post_time end) as question_post_time,
                  min(case when b.question_or_answer = 'answer' then post_time end) as answer_post_time,
                  response_time = min(response_time) over (partition by a.jeopardy_date, a.response) as first_responder
                FROM jeopardy.guesses a
                JOIN jeopardy.publish b
                ON a.jeopardy_date = b.jeopardy_date
                GROUP BY 1,2,3,4
            )
            
            SELECT a.*, b.name
            FROM results a
            JOIN (SELECT * FROM results WHERE first_responder = true) b
            ON a.jeopardy_date = b.jeopardy_date
            AND a.response = b.response
            WHERE a.response_time < a.answer_post_time
            AND EXTRACT(month from a.response_time) = EXTRACT(month from (current_date - interval '1 month'))
            AND EXTRACT(year from a.response_time) = EXTRACT(year from (current_date - interval '1 month'))
            ORDER BY a.jeopardy_date, a.name
            """
    elif period == 'all':
        q = """
            WITH results AS (
              SELECT a.jeopardy_date, a.name, a.response, a.response_time, 
                  min(case when b.question_or_answer = 'question' then full_text end) as question,
                  min(case when b.question_or_answer = 'answer' then full_text end) as answer,
                  min(case when b.question_or_answer = 'question' then post_time end) as question_post_time,
                  min(case when b.question_or_answer = 'answer' then post_time end) as answer_post_time,
                  response_time = min(response_time) over (partition by a.jeopardy_date, a.response) as first_responder
                FROM jeopardy.guesses a
                JOIN jeopardy.publish b
                ON a.jeopardy_date = b.jeopardy_date
                GROUP BY 1,2,3,4
            )
            
            SELECT a.*, b.name
            FROM results a
            JOIN (SELECT * FROM results WHERE first_responder = true) b
            ON a.jeopardy_date = b.jeopardy_date
            AND a.response = b.response
            WHERE a.response_time < a.answer_post_time
            ORDER BY a.jeopardy_date, a.name
            """
            
    rows = dbGetQuery(q)
    if len(rows) > 0:
        max_colnum = len(rows[0])-1
    else:
        max_colnum = 0

    for r,row in enumerate(rows):
        sim = similarity(row[2], row[5])
        rows[r] = rows[r] + (sim,)
            
        if (sim >= full_thresh):
            rows[r] = rows[r] + (True,)
        else:
            rows[r] = rows[r] + (False,)
            
        if (sim >= half_thresh) & (sim < full_thresh):
            rows[r] = rows[r] + (True,)
        else:
            rows[r] = rows[r] + (False,)

    out = dict()
    names = set([c[1] for c in rows])
    for name in names:
        out[name] = dict()
        out[name]['num_answers'] = sum([1 for c in rows if c[1] == name])

        out[name]['bandwagoned'] = [True for c in rows if (c[1] == name) & (not c[8])]
        out[name]['bandwagoned_ex'] = [(c[2],c[5]) for c in rows if (c[1] == name) & (not c[8])]
        out[name]['bandwagoned_correct'] = [c[max_colnum+2] for c in rows if (c[1] == name) & (not c[8])]
        out[name]['bandwagoned_first_responder'] = [c[9] for c in rows if (c[1] == name) & (not c[8])]
        
        jds = [c[0] for c in rows if c[1] == name]
        for jd in jds:
            max_sim = max([c[max_colnum+1] for c in rows if (c[1] == name) & (c[0] == jd)])
            rows = [rows[i] for i in range(len(rows)) if ((rows[i][1] == name) & (rows[i][0] == jd) & (rows[i][max_colnum+1] == max_sim)) or (rows[i][1] != name) or (rows[i][0] != jd)]
        
        
        out[name]['similarities'] = [c[max_colnum+1] for c in rows if c[1] == name]
        out[name]['num_questions'] = len(set([c[0] for c in rows if c[1] == name]))
        out[name]['correct'] = [c[max_colnum+2] for c in rows if c[1] == name]
        out[name]['correct_ex'] = [(c[2],c[5]) for c in rows if (c[1] == name) & (c[max_colnum+2])]
        out[name]['correct_ex_ss'] = [c[max_colnum+1] for c in rows if (c[1] == name) & (c[max_colnum+2])]
        out[name]['half_correct'] = [c[max_colnum+3] for c in rows if c[1] == name]
        out[name]['half_correct_ex'] = [(c[2],c[5]) for c in rows if (c[1] == name) & (c[max_colnum+3])]
        out[name]['half_correct_ex_ss'] = [c[max_colnum+1] for c in rows if (c[1] == name) & (c[max_colnum+3])]
        out[name]['avg_similarity'] = sum(out[name]['similarities']) / len(out[name]['similarities'])
        out[name]['num_correct'] = sum(out[name]['correct'])
        out[name]['num_bandwagoned'] = sum(out[name]['bandwagoned'])
        out[name]['hit_rate'] = out[name]['num_correct'] / out[name]['num_questions']
        out[name]['num_half_correct'] = sum(out[name]['half_correct'])
        out[name]['avg_num_answers'] = out[name]['num_answers'] / out[name]['num_questions']
        
    return(out, rows)
    
def create_leaderboard_text(metric, title, num_top, out, ex_name = None):
    txt = '{}\n'.format(title)
    txt += '------------------------------\n'
    
    s = [out[c]['num_questions'] for c in out.keys()]
    a = sorted(range(len(s)), key=lambda k: s[k], reverse = True)
    
    key_order = [list(out.keys())[c] for c in a]
    s = [out[c][metric] for c in key_order]
    a = sorted(range(len(s)), key=lambda k: s[k], reverse = True)
    
    for k,name in enumerate([key_order[c] for c in a][:num_top]):
        if metric == 'hit_rate':
            if ex_name is not None:
                txt += ('{:3}. {:20}: {:2.0f}\t({:.0f} / {:.0f})\n'.format(k+1, name, out[name][metric], out[name]['num_correct'], out[name]['num_questions']))
            else:
                txt += ('{:3}. {:20}: {:5.2f}\t({:.0f} / {:.0f})\n'.format(k+1, name, out[name][metric], out[name]['num_correct'], out[name]['num_questions']))
        else:
            if out[name][metric] > 0:
                if ex_name is not None:
                    txt += ('{:3}. {:20}: {:2.0f}'.format(k+1, name, out[name][metric]))
                else:
                    txt += ('{:3}. {:20}: {:5.2f}'.format(k+1, name, out[name][metric]))
                if (out[name][metric] > 0) & (ex_name is not None):
                    if len(out[name][ex_name]) > 0:
                        if metric == 'num_correct':
                            txt += '\t(most recent: "{}" == "{}" (ss = {:.2f}))\n'.format(out[name][ex_name][-1][0], out[name][ex_name][-1][1], out[name]['{}_ss'.format(ex_name)][-1])
                        elif metric == 'num_half_correct':
                            txt += '\t(most recent: "{}" ~= "{}" (ss = {:.2f}))\n'.format(out[name][ex_name][-1][0], out[name][ex_name][-1][1], out[name]['{}_ss'.format(ex_name)][-1])
                        elif metric == 'num_bandwagoned':
                            if out[name]['bandwagoned_correct'][-1]:
                                txt += '\t(most recent: "{:50} - jumped on {}\'s bandwagon, which was correct)\n'.format(out[name][ex_name][-1][0][:50] + '"', out[name]['bandwagoned_first_responder'][-1])
                            else:
                                txt += '\t(most recent: "{:50} - jumped on {}\'s bandwagon, which was not even correct)\n'.format(out[name][ex_name][-1][0][:50] + '"', out[name]['bandwagoned_first_responder'][-1])
                    else:
                        txt += '\n'
                else:
                    txt += '\n'
                
    txt += ('\n')
    
    return(txt)
    
def create_streak_text(rows, num_streak_to_show, full, complete_list):
    streak = 0
    txt = ''
    list_txt = ''
    
    max_colnum = 9
        
    if full:
        for k,dt in enumerate(sorted(list(set([c[0] for c in rows])), reverse = True)):
            
            if (sum([c[max_colnum+2] for c in rows if (c[0] == dt)]) > 0):
                streak += 1
                correct_names = [c[1] for c in rows if (c[0] == dt) & (c[max_colnum+2])]
                correct_answers = [c[2] for c in rows if (c[0] == dt) & (c[max_colnum+2])]
                if k < num_streak_to_show:
                    list_txt += '{}: {} answered correctly with "{}"\n'.format(dt, ' & '.join(correct_names), '" & "'.join(correct_answers))
            else:
                break
    else:
        for k,dt in enumerate(sorted(list(set([c[0] for c in rows])), reverse = True)):
            if (sum([(c[max_colnum+2] | c[max_colnum+3]) for c in rows if (c[0] == dt)]) > 0):
                streak += 1
                correct_names = [c[1] for c in rows if (c[0] == dt) & (c[max_colnum+2])]
                if len(correct_names) == 0:
                    correct_names = [c[1] for c in rows if (c[0] == dt) & (c[max_colnum+3])]
                    correct_answers = [c[2] for c in rows if (c[0] == dt) & (c[max_colnum+3])]
                    if k < num_streak_to_show:
                        list_txt += '{}: {} answered at least half-correctly with "{}"\n'.format(dt, ' & '.join(correct_names), '" & "'.join(correct_answers))
                else:
                    correct_answers = [c[2] for c in rows if (c[0] == dt) & (c[max_colnum+2])]
                    if k < num_streak_to_show:
                        list_txt += '{}: {} answered correctly with "{}"\n'.format(dt, ' & '.join(correct_names), '" & "'.join(correct_answers))
            else:
                break
    
    if k > num_streak_to_show:
        list_txt += ' ...and {} more...\n'.format(streak - num_streak_to_show)
        
    if complete_list:
        if full:
            txt += 'Current full-point streak is: {}\n'.format(streak)
            txt += '------------------------------\n'        
            txt += list_txt
            txt += '------------------------------\n'
    
            txt += ('\n')
        else:
            txt += 'Current half-point-or-better streak is: {}\n'.format(streak)
            txt += '------------------------------\n'
            txt += list_txt
            txt += '------------------------------\n'
    else:
        if full:
            txt += 'Current full-point streak is: {}\n'.format(streak)
        else:
            txt += 'Current half-point-or-better streak is: {}\n'.format(streak)
            
        txt += '------------------------------\n'
        
    return(txt, streak)

def process_payload(payload):

    print(payload)
    if 'EventSource' in payload.keys():
        if payload['EventSource'] == 'aws:sns':
            slack_payload = dict()
            ts = payload['Sns']['Timestamp']
            ts = datetime.datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S.%fZ')
            ti = (ts - datetime.datetime(1970,1,1)).total_seconds()

            slack_payload['event_ts'] = ti
            
            payload = json.loads(payload['Sns']['Message'])
            # It's a text - restructure it to look like a slack message
            slack_payload['user_id'] = payload['originationNumber']
            slack_payload['msg'] = payload['messageBody'].strip().lower()
            slack_payload['originating_channel'] = os.environ['finaljeopardychannelid']
            
            if slack_payload['user_id'] not in subscriber_list.keys():
                print('Unauthorized SMS user!')
                return {'statusCode': 200,'body': 'HTTP 200 OK'}
                
            payload = slack_payload
            print(payload)
            

    if ('bot_id' in payload.keys()) and (payload['bot_id'] == os.environ['bot_id']):
        # This message is from the bot itself, ignore
        return {'statusCode': 200,'body': 'HTTP 200 OK'}
    
    user_id = payload['user_id']
    msg = payload['msg']
    originating_channel = payload['originating_channel']
    posting_username = get_user_info(user_id)['user']['real_name']
    is_sms = (user_id[0] == '+')
    
    event_ts = datetime.datetime.utcfromtimestamp(payload['event_ts']).replace(tzinfo=pytz.utc)
    event_ts = event_ts.astimezone(pytz.timezone('US/Eastern'))

    if re.search('^my answer is', msg):
        td = datetime.datetime.now(pytz.timezone('US/Eastern'))
        tdd = td.date()
        rundate = prev_weekday(tdd)
        response = re.sub('[^0-9a-zA-Z ]+', '', msg)
        response = re.sub('^my answer is', '', response).strip()
        
        q = """SELECT * FROM jeopardy.publish WHERE post_time = (SELECT max(post_time) FROM jeopardy.publish WHERE question_or_answer = 'question')"""
        most_recent_posted_question = dbGetQuery(q)

        record_guess(user_id, most_recent_posted_question[0][0], event_ts, response, originating_channel, is_sms) 

    if (re.search('^babs answer is', re.sub('[^0-9a-zA-Z ]+', '', msg))) and (posting_username == 'Luke Kavenagh'):
        td = datetime.datetime.now(pytz.timezone('US/Eastern'))
        tdd = td.date()
        rundate = prev_weekday(tdd)
        response = re.sub('[^0-9a-zA-Z ]+', '', msg)
        response = re.sub('^babs answer is', '', response).strip()
        
        q = """SELECT * FROM jeopardy.publish WHERE post_time = (SELECT max(post_time) FROM jeopardy.publish WHERE question_or_answer = 'question')"""
        most_recent_posted_question = dbGetQuery(q)

        record_guess('Barbara Evangelista', most_recent_posted_question[0][0], event_ts, response, originating_channel)
        
    if (re.search('^reeds moms answer is', re.sub('[^0-9a-zA-Z ]+', '', msg))):
        td = datetime.datetime.now(pytz.timezone('US/Eastern'))
        tdd = td.date()
        rundate = prev_weekday(tdd)
        response = re.sub('[^0-9a-zA-Z ]+', '', msg)
        response = re.sub('^reeds moms answer is', '', response).strip()
        
        q = """SELECT * FROM jeopardy.publish WHERE post_time = (SELECT max(post_time) FROM jeopardy.publish WHERE question_or_answer = 'question')"""
        most_recent_posted_question = dbGetQuery(q)

        record_guess("Reed''s Mom", most_recent_posted_question[0][0], event_ts, response, originating_channel)

    if re.sub('[^a-z ]', '', msg) == "yesterdays question":
        td = datetime.datetime.now(pytz.timezone('US/Eastern'))
        tdd = td.date()
        rundate = prev_weekday(tdd)
        dat = get_jeopardy_data(rundate)

        record_publish(rundate, 'question', dat['question_raw'], td)
        post_msg = "{} requested yesterday's question:\n---------------------\n{}".format(posting_username, dat['question'])
        post_message_to_channel(post_msg, originating_channel)
        send_sms_to_all(post_msg)
        
    if re.sub('[^a-z ]', '', msg) == "yesterdays answer":
        td = datetime.datetime.now(pytz.timezone('US/Eastern'))
        tdd = td.date()
        rundate = prev_weekday(tdd)
        dat = get_jeopardy_data(rundate)

        if (originating_channel != os.environ['finaljeopardychannelid']) and (user_id != os.environ['slacklukeid']):
            image_url = "https://media.giphy.com/media/FmyCxAjnOP5Di/giphy.gif"
            attachments = [{"title": "Newman", "image_url": image_url}]
            post_message_to_channel('Uh uh uh', originating_channel, attachments)
        else:
            post_msg = "{} requested yesterday's answer\n---------------------\n{}\n---------------------\n{}".format(posting_username, dat['answer'], dat['extended_answer'])
            record_publish(rundate, 'answer', dat['answer'], td)
            post_message_to_channel(post_msg, originating_channel)
            send_sms_to_all(post_msg)
    
    if re.search("\\d{4}-\\d{2}-\\d{2} question", msg):
        datestr = re.search("(\\d{4}-\\d{2}-\\d{2}) question", msg).group(1)
        rundate = datetime.datetime.strptime(datestr, '%Y-%m-%d')
        td = datetime.datetime.now(pytz.timezone('US/Eastern'))
        
        dat = get_jeopardy_data(rundate)
        
        if dat['question'] is None:
            post_message_to_channel('No final jeopardy found for {:%A, %b %#d, %Y}'.format(rundate), originating_channel)
        else:
            post_msg = "{} requested the {} question:\n---------------------\n{}".format(posting_username, rundate.date(), dat['question'])
            record_publish(rundate, 'question', dat['question_raw'], td)
            post_message_to_channel(post_msg, originating_channel)
            send_sms_to_all(post_msg)
            
    if re.search("\\d{4}-\\d{2}-\\d{2} answer", msg):
        datestr = re.search("(\\d{4}-\\d{2}-\\d{2}) answer", msg).group(1)
        rundate = datetime.datetime.strptime(datestr, '%Y-%m-%d')
        td = datetime.datetime.now(pytz.timezone('US/Eastern'))
        
        dat = get_jeopardy_data(rundate)
        
        if dat['answer'] is None:
            post_message_to_channel('No final jeopardy found for {:%A, %b %#d, %Y}'.format(rundate), originating_channel)
        else:
            if (originating_channel != os.environ['finaljeopardychannelid']) and (originating_channel != os.environ['luketestchannelid']) and (user_id != os.environ['slacklukeid']):
                image_url = "https://media.giphy.com/media/FmyCxAjnOP5Di/giphy.gif"
                attachments = [{"title": "Newman", "image_url": image_url}]
                post_message_to_channel('Uh uh uh', originating_channel, attachments)
            else:
                post_msg = "{} requested the {} questions:\n---------------------\n{}\n---------------------\n{}".format(posting_username, rundate.date(), dat['answer'], dat['extended_answer'])
                record_publish(rundate, 'answer', dat['answer'], td)
                post_message_to_channel(post_msg, originating_channel)
                send_sms_to_all(post_msg)
                
    if re.search('^show (scores|results) for', msg):
        if re.search("(\\d{4}-\\d{2}-\\d{2})", msg):
            datestr = re.search("(\\d{4}-\\d{2}-\\d{2})", msg).group(1)
            rundate = datetime.datetime.strptime(datestr, '%Y-%m-%d').date()
        elif re.search('today', msg):
            td = datetime.datetime.now(pytz.timezone('US/Eastern')).date()
            rundate = prev_weekday(td)
        elif re.search('yesterday', msg):
            td = datetime.datetime.now(pytz.timezone('US/Eastern')).date()
            rundate = prev_weekday(prev_weekday(td))
        else:
            return(None)
        q = """
            SELECT * FROM (
                SELECT a.jeopardy_date, a.name, a.response, a.response_time, 
                  min(case when b.question_or_answer = 'question' then full_text end) as question,
                  min(case when b.question_or_answer = 'answer' then full_text end) as answer,
                  min(case when b.question_or_answer = 'question' then post_time end) as question_post_time,
                  min(case when b.question_or_answer = 'answer' then post_time end) as answer_post_time
                FROM jeopardy.guesses a
                JOIN jeopardy.publish b
                ON a.jeopardy_date = b.jeopardy_date
                GROUP BY 1,2,3,4
            ) a
            WHERE response_time < answer_post_time
            AND jeopardy_date = '{}'
            ORDER BY name ASC, response_time ASC
            """.format(rundate)
        print(q)
        rows = dbGetQuery(q)
        
        if len(rows) == 0:
            if is_sms:
                send_sms(user_id, 'No results found for {}'.format(rundate))
            else:
                post_message_to_channel('No results found for {}'.format(rundate), originating_channel)
                
            return(None)
            
        if is_sms:
            txt = '```Results for {}\n'.format(rundate)
            txt += '------------------------------\n'
            cur_user = ''
            for i,row in enumerate(rows):
                if (i > 0) and (row[1] != cur_user):
                    cur_user = row[1]
                elif i == 0:
                    cur_user = row[1]
                sim = similarity(row[2], row[5])
                txt += '{:20} {:25} {:2.2f}\n'.format(row[1], row[2][:25], sim)
    
            txt += '```'
            send_sms(user_id, txt)
        else:    
            txt = '```Results for {}\n'.format(rundate)
            txt += '------------------------------\n'
            txt += 'Question: {}\n'.format(rows[0][4])
            txt += 'Answer:   {}\n\n'.format(rows[0][5])
            txt += '(answer given at {:%H:%M:%S})\n'.format(rows[0][7])
            txt += '------------------------------\n\n'
            txt += '{:15}   {:20}   {:50}   {:16}\n'.format('Response time', 'Name', 'Best answer', 'Similarity Score')
            txt += '{:15}   {:20}   {:50}   {:16}\n'.format('-------------', '----', '-----------', '----------------')
            cur_user = ''
            for i,row in enumerate(rows):
                if (i > 0) and (row[1] != cur_user):
                    cur_user = row[1]
                    txt += '\n'
                elif i == 0:
                    cur_user = row[1]
                sim = similarity(row[2], row[5])
                txt += '{:%H:%M:%S}          {:20}   {:50}   {:16.2f}\n'.format(row[3], row[1], row[2][:50], sim)
    
            txt += '```'
            post_message_to_channel(txt, originating_channel)
        
    if re.search('^show my recent (scores|results)', msg):
        q = """
            SELECT * FROM (
                SELECT a.jeopardy_date, a.name, a.response, a.response_time, 
                  min(case when b.question_or_answer = 'question' then full_text end) as question,
                  min(case when b.question_or_answer = 'answer' then full_text end) as answer,
                  min(case when b.question_or_answer = 'question' then post_time end) as question_post_time,
                  min(case when b.question_or_answer = 'answer' then post_time end) as answer_post_time
                FROM jeopardy.guesses a
                JOIN jeopardy.publish b
                ON a.jeopardy_date = b.jeopardy_date
                GROUP BY 1,2,3,4
            ) a
            WHERE response_time < answer_post_time
            AND name = '{}'
            ORDER BY jeopardy_date DESC
            """.format(posting_username)
        rows = dbGetQuery(q)
        
        p = 0
        max_sim = 0

        if is_sms:
            txt = ''
            for i,row in enumerate(rows):
                if (i > 0) and (row[0] < last_question_dt):
                    txt += tmp
                    p += 1
                    last_question_dt = row[0]
                    max_sim = -1
                    tmp = None
                    
    
                sim = similarity(row[2], row[5])
                if sim > max_sim:
                    tmp = '{:10} {:25} {:2.2f}\n'.format(str(row[0]), row[2][:25], sim)
                    max_sim = sim
                if i == 0:
                    last_question_dt = row[0]
                if p >= 3:
                    break
                
            send_sms(user_id, txt)
        else:
            txt = '```Recent results for {}\n'.format(posting_username)
            txt += '------------------------------\n\n'
            txt += '{:10}   {:50}   {:50}   {:50}   {:16}\n'.format('Date', 'Question', 'Answer', 'My (best) answer', 'Similarity Score')
            txt += '{:10}   {:50}   {:50}   {:50}   {:16}\n'.format('----', '--------', '------', '----------------', '----------------')
            
            for i,row in enumerate(rows):
                if (i > 0) and (row[0] < last_question_dt):
                    txt += tmp
                    p += 1
                    last_question_dt = row[0]
                    max_sim = -1
                    tmp = None
                    
    
                sim = similarity(row[2], row[5])
                if sim > max_sim:
                    tmp = '{:10}   {:47}...   {:50}   {:50}   {:16.2f}\n'.format(str(row[0]), row[4][:47], row[5][:50], row[2][:50], sim)
                    max_sim = sim
                if i == 0:
                    last_question_dt = row[0]
                    
                if p >=5:
                    break
                
            txt += '```'
            post_message_to_channel(txt, originating_channel)

    if re.search('^leaderboard', msg):
        out,rows = get_leaderboard_data('month')

        txt = '``` Leaderboard for {:%B %Y}\n\n'.format(datetime.datetime.now())

        txt += create_leaderboard_text('num_correct', 'Number of correct answers (top 10)', 10, out, 'correct_ex')
        txt += create_leaderboard_text('num_bandwagoned', 'Number of bandwagoned answers (top 10)', 10, out, 'bandwagoned_ex')
        txt += create_leaderboard_text('hit_rate', 'Best hit rate (top 10)', 10, out)
        txt += create_leaderboard_text('avg_similarity', 'Average similarity score (top 5)', 5, out)
        txt += create_leaderboard_text('num_half_correct', 'Number of half-correct answers (top 3)', 3, out, 'half_correct_ex')

        out,rows = get_leaderboard_data('all')
        
        full_streak_txt, full_streak = create_streak_text(rows, num_streak_to_show = 3, full = True, complete_list = True)
        full_streak_txt_short, full_streak = create_streak_text(rows, num_streak_to_show = 3, full = True, complete_list = False)
        
        half_streak_txt, half_streak = create_streak_text(rows, num_streak_to_show = 3, full = False, complete_list = True)
        half_streak_txt_short, half_streak = create_streak_text(rows, num_streak_to_show = 3, full = False, complete_list = False)
        
        if full_streak > 0:
            txt += full_streak_txt
        else:
            txt += full_streak_txt_short
            
        txt += ('\n')
        
        if (full_streak > -1) & (half_streak > 0):
            txt += half_streak_txt
        else:
            txt += half_streak_txt_short
        
        txt += '```'
        
        if is_sms:
            out,rows = get_leaderboard_data('month')
            txt = create_leaderboard_text('num_correct', 'Number of correct answers (top 10)', 10, out)
            txt += '\n'
            txt += 'Streak is at {}'.format(full_streak)
            send_sms(user_id, txt)
        else:
            post_message_to_channel(txt, originating_channel)
    
    if re.search('^last leaderboard', msg):
        out,rows = get_leaderboard_data('lastmonth')

        txt = '``` Leaderboard for {:%B %Y}\n\n'.format(datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1))

        txt += create_leaderboard_text('num_correct', 'Number of correct answers (top 10)', 10, out, 'correct_ex')
        txt += create_leaderboard_text('num_bandwagoned', 'Number of bandwagoned answers (top 10)', 10, out, 'bandwagoned_ex')
        txt += create_leaderboard_text('hit_rate', 'Best hit rate (top 10)', 10, out)
        txt += create_leaderboard_text('avg_similarity', 'Average similarity score (top 5)', 5, out)
        txt += create_leaderboard_text('num_half_correct', 'Number of half-correct answers (top 3)', 3, out, 'half_correct_ex')

        txt += '```'
        
        if is_sms:
            out,rows = get_leaderboard_data('lastmonth')
            txt = create_leaderboard_text('num_correct', 'Number of correct answers (top 10)', 10, out, 'correct_ex')
            send_sms(user_id, txt)
        else:
            post_message_to_channel(txt, originating_channel)
        
    if re.search("^((all time)|(alltime)|(all-time)) leaderboard", msg):
        out,rows = get_leaderboard_data('all')

        txt = '``` All-time leaderboard\n\n'
        
        txt += create_leaderboard_text('num_correct', 'Number of correct answers (top 10)', 10, out, 'correct_ex')
        txt += create_leaderboard_text('num_bandwagoned', 'Number of bandwagoned answers (top 10)', 10, out, 'bandwagoned_ex')
        txt += create_leaderboard_text('hit_rate', 'Best hit rate (top 10)', 10, out)
        txt += create_leaderboard_text('avg_similarity', 'Average similarity score (top 5)', 5, out)
        txt += create_leaderboard_text('num_half_correct', 'Number of half-correct answers (top 3)', 3, out, 'half_correct_ex')
        
        full_streak_txt, full_streak = create_streak_text(rows, num_streak_to_show = 3, full = True, complete_list = True)
        full_streak_txt_short, full_streak = create_streak_text(rows, num_streak_to_show = 3, full = True, complete_list = False)
        
        half_streak_txt, half_streak = create_streak_text(rows, num_streak_to_show = 3, full = False, complete_list = True)
        half_streak_txt_short, half_streak = create_streak_text(rows, num_streak_to_show = 3, full = False, complete_list = False)
        
        if full_streak > 0:
            txt += full_streak_txt
        else:
            txt += full_streak_txt_short
            
        txt += ('\n')
        
        if (full_streak > -1) & (half_streak > 0):
            txt += half_streak_txt
        else:
            txt += half_streak_txt_short
        
        txt += '```'
        
        if is_sms:
            out,rows = get_leaderboard_data('all')
            txt = create_leaderboard_text('num_correct', 'Number of correct answers (top 10)', 10, out, 'correct_ex')
            send_sms(user_id, txt)
        else:
            post_message_to_channel(txt, originating_channel)

    if re.search('^help', msg):
        if is_sms:
            txt = """jeopardy-bot commands:
            -yesterday's question
            -yesterday's answer
            -YYYY-MM-DD question
            -YYYY-MM-DD answer
            -my answer is <answer>
            -leaderboard
            -last leaderboard
            -all time leaderboard
            -show my recent scores
            -show scores for today
            """
            send_sms(user_id, txt)
        else:
            txt = """jeopardy-bot commands:
                @jeopardybot yesterday's question
                @jeopardybot yesterday's answer
                @jeopardybot YYYY-MM-DD question
                @jeopardybot YYYY-MM-DD answer
                @jeopardybot my answer is <answer>
                @jeopardybot leaderboard
                @jeopardybot last leaderboard
                @jeopardybot all time leaderboard
                @jeopardybot show my recent scores
                @jeopardybot show scores for today
            """
            post_message_to_channel(txt, originating_channel)

def lambda_handler(event, context):
    print(event)
    if ('body' in event.keys()) and (event['body'] is not None) and ('type' in json.loads(event['body'])) and (json.loads(event['body'])['type'] == 'url_verification'):
        s = """HTTP 200 OK
                Content-type: text/plain
                {}""".format(json.loads(event['body'])['challenge'])
        return {
            'statusCode': 200,
            'body': s
        }

    elif ('headers' in event.keys()) and ('User-Agent' in event['headers']) and (re.search('Slackbot', (event['headers']['User-Agent']))):
        # This event is coming from Slack!
        add_to_queue = False
        if ('subtype' in json.loads(event['body'])['event']) and (json.loads(event['body'])['event']['subtype'] == 'message_changed'):
            # message was edited, still test if the most recent edit was directed and add to queue if so
            if re.search(r'^<@{}>'.format(os.environ['slackjeopardybotid']), json.loads(event['body'])['event']['message']['text']):
                add_to_queue = True
                user_id = json.loads(event['body'])['event']['message']['user']
                msg = json.loads(event['body'])['event']['message']['text'].replace(r'<@{}>'.format(os.environ['slackjeopardybotid']), '').strip().lower()
                event_ts = float(json.loads(event['body'])['event']['message']['edited']['ts'])
                originating_channel = json.loads(event['body'])['event']['channel']

        elif re.search(r'^<@{}>'.format(os.environ['slackjeopardybotid']), json.loads(event['body'])['event']['text']) :
            # It's a directed message, add it to our SQS queue
            add_to_queue = True
            user_id = json.loads(event['body'])['event']['user']
            msg = json.loads(event['body'])['event']['text'].replace(r'<@{}>'.format(os.environ['slackjeopardybotid']), '').strip().lower()
            event_ts = float(json.loads(event['body'])['event']['event_ts'])
            originating_channel = json.loads(event['body'])['event']['channel']
            
        if add_to_queue:
            print('Sending Slack event to the queue: {}'.format(event))
            sqs = boto3.client('sqs')
    
            signature = event['headers']['X-Slack-Signature']
            req = 'v0:' + event['headers']['X-Slack-Request-Timestamp'] + ':' + event['body']
            request_hash = 'v0=' + hmac.new(
                os.environ['signing_secret'].encode(),
                req.encode(), hashlib.sha256
            ).hexdigest()
            
            if hmac.compare_digest(request_hash, signature):
                payload = dict({'user_id':user_id, 
                                'msg':msg, 
                                'originating_channel':originating_channel,
                                'event_ts': event_ts})
                response = sqs.send_message(
                    QueueUrl=os.environ['sqs_url'],
                    MessageBody=json.dumps(payload)
                )
            else:
                print('Message was unverifiable!\nevent:{}'.format(json.dumps(event)))
            return {'statusCode': 200,'body': 'HTTP 200 OK'}
        else:
            return {'statusCode': 200,'body': 'HTTP 200 OK'}
        
    elif 'Records' in event.keys():
        # This is from either our SQS queue or from SNS (text), process it
        
        for record in event['Records']:
            if 'body' in record.keys():
                print('Processing an event from SQS')
                process_payload(json.loads(record['body']))
            else:
                print('Processing an event from SNS')
                process_payload(record)
        return {'statusCode': 200,'body': 'HTTP 200 OK'}
        
    else:
        return {'statusCode': 200,'body': 'Hello, world!'}
