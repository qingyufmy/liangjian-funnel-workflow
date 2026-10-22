"""Authored-role fixtures speak the same SSE wire as the configured gateway.

Only the HTTP envelope changes; original model content and every business
assertion stay untouched. Protocol/JSON fallback tests use their own transports.
"""
import json as codec
import httpx


def sse_model_response(status_code=200, *, json, request=None, headers=None, **kwargs):
    events=[]
    for choice in json['choices']:
        events.append({'choices':[{**{k:v for k,v in choice.items() if k!='message'},
                                  'delta':choice.get('message',{})}]})
    if isinstance(json.get('usage'),dict):
        events.append({'choices':[],'usage':json['usage']})
    content=''.join('data: '+codec.dumps(value)+'\n\n' for value in events)+'data: [DONE]\n\n'
    return httpx.Response(status_code,content=content,headers={**(headers or {}),'content-type':'text/event-stream'},request=request,**kwargs)
