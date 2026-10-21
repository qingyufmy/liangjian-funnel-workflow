// Visual source: Claude ROLE_CARDS_2.0.md (2026-10-19), not strategy or identity data.
export const ROLES=Object.freeze([
  {id:'a1',ch:'基',name:'基本面研究员',short:'基本面',color:'#6D4FD8',hex:0x6D4FD8,hair:0xB3B8C0,style:'grey',glasses:'square',cues:['方框眼镜、灰白短发','线框地球仪、年报'],work:'转地球仪'},
  {id:'a2',ch:'题',name:'题材研究员',short:'题材',color:'#0E8C96',hex:0x0E8C96,hair:0x222326,style:'pony',cues:['高马尾、颈挂耳机','三联迷你屏、黄色工牌'],work:'快速敲键'},
  {id:'pm',ch:'组',name:'组合经理',short:'组合',color:'#2F6FD6',hex:0x2F6FD6,hair:0x2B2420,style:'short',glasses:'round',cues:['领带、圆框眼镜、俯看桌面','白瓷杯、资金表盘'],work:'翻计划'},
  {id:'tr',ch:'交',name:'交易员',short:'交易员',color:'#E0A100',hex:0xE0A100,hair:0x2B2420,style:'short',headset:true,cues:['耳麦、马甲、卷袖露前臂','双屏、物理急停按钮'],work:'双手敲键'},
  {id:'rk',ch:'控',name:'风控师',short:'风控',color:'#435474',hex:0x435474,hair:0x2B2420,style:'flat',beard:true,cues:['短胡须、抱臂','盾牌摆件、拦截护罩'],work:'逐行核对'},
  {id:'rv',ch:'复',name:'复盘师',short:'复盘',color:'#2E9B5F',hex:0x2E9B5F,hair:0x6B4232,style:'bun',glasses:'thin',cues:['盘发、细框眼镜、笔','红蓝金三本复盘本'],work:'翻本子'}
]);
export const STATES=Object.freeze(['idle','work','talk','stop','happy','rest']);
export const STATE_LABELS=Object.freeze({idle:'待命',work:'工作中',talk:'发言中',stop:'停止',happy:'完成',rest:'休息'});
