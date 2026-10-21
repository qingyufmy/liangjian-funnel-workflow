import {createHmac,randomBytes,timingSafeEqual} from 'node:crypto';
import type {Request,RequestHandler} from 'express';
import {tokenMatches} from './auth.js';

const cookieName='liangjian_team_ui';
const lifetime=8*60*60*1000;
const equal=(a:string,b:string)=>a.length===b.length&&timingSafeEqual(Buffer.from(a),Buffer.from(b));

/** Reuses the configured internal dashboard token; no account or DB is created. */
export function createTeamUiAccess(token:string|null,now=Date.now){
  const key=randomBytes(32); // In-memory only. A Node restart invalidates UI sessions.
  const sign=(value:string)=>createHmac('sha256',key).update(value).digest('hex');
  const valid=(request:Request)=>{
    if(!token)return false; // UI fails closed even if the legacy console has no token.
    if(tokenMatches(request,token))return true;
    const cookie=request.headers.cookie?.split(';').map(x=>x.trim()).find(x=>x.startsWith(cookieName+'='))?.slice(cookieName.length+1);
    if(!cookie||!/^\d{13}\.[a-f0-9]{32}\.[a-f0-9]{64}$/.test(cookie))return false;
    const split=cookie.lastIndexOf('.'),value=cookie.slice(0,split),expires=Number(value.split('.')[0]);
    return expires>now()&&expires<=now()+lifetime&&equal(cookie.slice(split+1),sign(value));
  };
  const api:RequestHandler=(request,response,next)=>{
    response.setHeader('Cache-Control','no-store');
    if(!valid(request)){response.status(401).json({error:'UNAUTHORIZED'});return;}
    next();
  };
  const page:RequestHandler=(request,response,next)=>{
    response.setHeader('Cache-Control','no-store');
    if(!valid(request)){response.redirect(303,'/team/login');return;}next();
  };
  const loginPage:RequestHandler=(_request,response)=>{
    response.setHeader('Cache-Control','no-store');
    response.setHeader('Referrer-Policy','no-referrer');
    response.setHeader('Content-Security-Policy',"default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'");
    response.type('html').send(`<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>量见 · 内部登录</title><style>body{margin:0;background:#f3f4f6;color:#12161d;font:16px/1.6 system-ui,sans-serif}main{max-width:360px;margin:12vh auto;padding:24px}h1{font-size:24px}p{color:#5b6472}label{display:block}input,button{box-sizing:border-box;width:100%;min-height:44px;font:inherit;border:1px solid #c9d1dc;border-radius:8px;padding:10px;margin:8px 0 16px}button{background:#12161d;color:white;cursor:pointer}:focus-visible{outline:3px solid #f0b429;outline-offset:3px}</style><main><h1>量见团队 · 内部登录</h1><p>仅供内部查看。请输入现有控制台访问令牌；不创建新账户。</p><form method="post" action="/team/login"><label for="token">内部访问令牌</label><input id="token" name="token" type="password" required autocomplete="off" maxlength="512"><button type="submit">登录并查看团队</button></form></main></html>`);
  };
  const login:RequestHandler=(request,response)=>{
    response.setHeader('Cache-Control','no-store');
    const supplied=request.body?.token;
    if(!token||typeof supplied!=='string'||Buffer.byteLength(supplied)!==Buffer.byteLength(token)||!timingSafeEqual(Buffer.from(supplied),Buffer.from(token))){response.status(401).type('html').send('<!doctype html><html lang="zh-CN"><meta charset="utf-8"><title>登录未通过</title><p>访问令牌未通过验证。</p><a href="/team/login">返回登录</a></html>');return;}
    const value=String(now()+lifetime)+'.'+randomBytes(16).toString('hex');
    // Host-only, HttpOnly, SameSite. Never place the internal token in a cookie/URL.
    // Forwarded HTTPS may only tighten the cookie flag, never grant access.
    const secure=request.secure||request.header('x-forwarded-proto')==='https';
    response.cookie(cookieName,value+'.'+sign(value),{httpOnly:true,sameSite:'strict',secure,path:'/',maxAge:lifetime});
    response.redirect(303,'/team');
  };
  return {api,page,loginPage,login};
}
