// Visual source: Claude HOME_REFERENCE.dc.html stations / 0441-notice-design-reference.
// Styling metadata only, never strategy or identity data.
export const ROLES=Object.freeze([
  {id:'a1',ch:'基',name:'基本面研究员',short:'基本面',color:'#8B78E0',hex:0x8B78E0,hair:0xB9BDC4,style:'grey',glasses:'square',cues:['方框眼镜、灰白短发','线框地球仪、年报'],work:'转地球仪'},
  {id:'a2',ch:'题',name:'题材研究员',short:'题材',color:'#3FA3AD',hex:0x3FA3AD,hair:0x1E1A18,style:'pony',cues:['高马尾、颈挂耳机','三联迷你屏、黄色工牌'],work:'快速敲键'},
  {id:'pm',ch:'组',name:'组合经理',short:'组合',color:'#5C8BE6',hex:0x5C8BE6,hair:0x2B2724,style:'short',glasses:'round',cues:['领带、圆框眼镜、俯看桌面','白瓷杯、资金表盘'],work:'翻计划'},
  {id:'rk',ch:'控',name:'风控师',short:'风控',color:'#A7B2C6',hex:0xA7B2C6,hair:0x2B2724,style:'flat',beard:true,cues:['短胡须、抱臂','盾牌摆件、拦截护罩'],work:'逐行核对'},
  {id:'tr',ch:'交',name:'交易员',short:'交易员',color:'#E0A62B',hex:0xE0A62B,hair:0x2B2724,style:'short',headset:true,cues:['耳麦、马甲、卷袖露前臂','双屏、物理急停按钮'],work:'双手敲键'},
  {id:'rv',ch:'复',name:'复盘师',short:'复盘',color:'#55B282',hex:0x55B282,hair:0x4A3426,style:'bun',glasses:'thin',cues:['盘发、细框眼镜、笔','红蓝金三本复盘本'],work:'翻本子'}
]);
export const STATES=Object.freeze(['idle','work','talk','stop','happy','rest']);
export const STATE_LABELS=Object.freeze({idle:'待命',work:'工作中',talk:'发言中',stop:'拦截中',happy:'完成',rest:'休息'});
