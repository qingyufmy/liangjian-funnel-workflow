import {ROLES,STATES} from './roles.js';

/** Geometry-only visual layer. No fetch, ledger, order or strategy dependencies. */
export function createStage(host,{preview=false,onUnavailable=()=>{}}={}){
  const T=window.THREE;
  if(!T){onUnavailable('THREE_UNAVAILABLE');return null;}
  let renderer;
  try{renderer=new T.WebGLRenderer({antialias:true,alpha:false,preserveDrawingBuffer:preview});}
  catch{onUnavailable('WEBGL_UNAVAILABLE');return null;}
  renderer.setPixelRatio(Math.min(devicePixelRatio||1,1.6));
  renderer.shadowMap.enabled=true;renderer.shadowMap.type=T.PCFSoftShadowMap;
  // Match Claude r149 baseline's linear output; avoid a second gamma lift.
  renderer.outputEncoding=T.LinearEncoding;
  renderer.toneMapping=T.ACESFilmicToneMapping;renderer.toneMappingExposure=.9;
  host.append(renderer.domElement);
  const scene=new T.Scene(),camera=new T.PerspectiveCamera(29,1,.1,100);
  const materials=[],geometries=[],textures=[];
  const material=(color,extra={})=>{const v=new T.MeshStandardMaterial({color,roughness:.7,...extra});materials.push(v);return v;};
  const skin=material(0xF3CDA8),shirt=material(0xf6f7fa),ink=material(0x252930),metal=material(0xb5bfca,{metalness:.5,roughness:.4});
  const white=material(0xffffff),trouser=material(0x3c4451),glass=material(0x17202c),gold=material(0xe0a100);
  const environment=new T.Group();scene.add(environment);
  const M=(geo,mat,x,y,z,parent)=>{geometries.push(geo);const mesh=new T.Mesh(geo,mat);mesh.position.set(x,y,z);mesh.castShadow=true;mesh.receiveShadow=true;parent.add(mesh);return mesh;};
  const box=(w,h,d,mat,x,y,z,parent)=>M(new T.BoxGeometry(w,h,d),mat,x,y,z,parent);
  const ball=(r,mat,x,y,z,parent)=>M(new T.SphereGeometry(r,24,16),mat,x,y,z,parent);
  const cylinder=(r,h,mat,x,y,z,parent)=>M(new T.CylinderGeometry(r,r,h,20),mat,x,y,z,parent);
  const rod=(a,b,r,mat,parent)=>{const va=new T.Vector3(...a),vb=new T.Vector3(...b),dir=vb.clone().sub(va);const p=M(new T.CylinderGeometry(r,r,dir.length(),10),mat,...va.clone().add(vb).multiplyScalar(.5).toArray(),parent);p.quaternion.setFromUnitVectors(new T.Vector3(0,1,0),dir.normalize());return p;};
  const planeMat=material(0xe2e7ee),deskMat=material(0xfafbfc),darkDesk=material(0xd5dce7);
  box(19,.13,5.6,planeMat,0,-.1,.1,environment);
  const wallMat=material(0xcbd5e3);
  box(19,4.5,.13,wallMat,0,2,-2.25,environment);
  const hemi=new T.HemisphereLight(0xffffff,0xb6bcc8,.8);scene.add(hemi);
  const key=new T.DirectionalLight(0xffffff,.9);key.position.set(-4,8,7);key.castShadow=true;key.shadow.mapSize.set(2048,1024);Object.assign(key.shadow.camera,{left:-10,right:10,top:5,bottom:-5});key.shadow.camera.updateProjectionMatrix();key.shadow.bias=-.001;scene.add(key);
  const rim=new T.DirectionalLight(0xaec9ff,.35);rim.position.set(4,5,-3);scene.add(rim);
  const stations=new Map(),happyTimers=new Map();let focus='all',paused=preview,view='color',last=0,raf=0,visible=true,disposed=false;
  const reduceQuery=matchMedia('(prefers-reduced-motion: reduce)');let reduce=reduceQuery.matches;
  const shaderSilhouette=new T.MeshBasicMaterial({color:0x1b2028});materials.push(shaderSilhouette);
  function textScreen(role,width=320,height=170){
    const cv=document.createElement('canvas');cv.width=width;cv.height=height;const c=cv.getContext('2d');
    c.fillStyle='#17202c';c.fillRect(0,0,width,height);c.fillStyle=role.color;c.fillRect(0,0,width,5);
    c.fillStyle='#eaf0f7';c.font='600 20px "Microsoft YaHei",sans-serif';c.fillText(role.short,16,37);
    c.font='12px "Microsoft YaHei",sans-serif';c.fillStyle='#b2c1d3';c.fillText(preview?'设计样板 · 数值待来源':'等待带来源的事件',16,59);
    const line=(label,value,y)=>{c.fillStyle='#b2c1d3';c.fillText(label,16,y);c.fillStyle='#eaf0f7';c.textAlign='right';c.fillText(value,width-16,y);c.textAlign='left';};
    c.strokeStyle='#41536c';
    if(role.id==='a1'){
      for(const [i,label]of['全市场','财务初筛','主题链条','通过'].entries()){
        const w=width-34-i*30,x=(width-w)/2,y=72+i*22;c.strokeRect(x,y,w,18);c.fillStyle='#eaf0f7';c.fillText(label,x+7,y+13);c.textAlign='right';c.fillText('—',x+w-7,y+13);c.textAlign='left';}
    }else if(role.id==='a2'){
      const labels=role.screenKind==='emotion'?['涨停 —','破板 —','连板 —','跌停 —']:role.screenKind==='limitup'?['涨停池 · 缺原件','暂无可证列表','轮动速度 —']:['题材热度榜','强度 / 阶段均待源','不推演当日题材'];
      labels.forEach((label,i)=>line(label,'',86+i*24));
    }else if(role.id==='pm'){
      c.beginPath();c.arc(71,110,35,0,Math.PI*2);c.stroke();c.fillStyle='#eaf0f7';c.font='22px sans-serif';c.fillText('—',60,119);c.font='12px "Microsoft YaHei",sans-serif';line('资金使用率','—',84);line('槽位','— / —',112);line('市场状态','待来源',140);
    }else if(role.id==='tr'){
      if(role.screenKind==='price'){c.beginPath();c.moveTo(25,80);c.lineTo(25,145);c.lineTo(295,145);c.stroke();line('分时 · 无报价','—',94);line('无行情不画走势','',125);}
      else{line('现价 / 回踩区距离','—',86);line('入场条件','缺原件',112);line('浮动盈亏','—',138);}
    }else if(role.id==='rk'){
      ['单笔预算','现金底线','新开限制','同簇上限','截止时刻'].forEach((label,i)=>line(label,'—',79+i*17));
    }else if(role.id==='rv'){
      line('近期买点结果','—',88);line('最近点评','待原件',116);line('经验库','待来源',144);
    }else{line('实时来源','未连接',94);line('不补造行情或任务','',124);}
    const tex=new T.CanvasTexture(cv);textures.push(tex);const mat=new T.MeshBasicMaterial({map:tex});materials.push(mat);return mat;
  }
  function shield(mat,parent,x,y,z,scale=1){
    const shape=new T.Shape();shape.moveTo(0,.2);shape.lineTo(.17,.12);shape.lineTo(.13,-.09);shape.lineTo(0,-.23);shape.lineTo(-.13,-.09);shape.lineTo(-.17,.12);shape.closePath();
    const m=M(new T.ExtrudeGeometry(shape,{depth:.045,bevelEnabled:false}),mat,x,y,z,parent);m.scale.setScalar(scale);return m;
  }
  function screen(role,parent,x,y,z,w=.65,h=.42){
    rod([x,y-.28,z],[x,y,z],.025,metal,parent);box(w+.045,h+.045,.055,ink,x,y,z,parent);
    const m=M(new T.PlaneGeometry(w,h),textScreen(role),x,y,z+.031,parent);return m;
  }
  for(const [i,r]of ROLES.entries()){
    const g=new T.Group();g.position.set((i-2.5)*2.72,0,.055*(i-2.5)**2);g.rotation.y=-g.position.x*.018;scene.add(g);
    const color=material(r.hex),hair=material(r.hair),character=new T.Group();g.add(character);
    cylinder(.98,.08,deskMat,0,0,0,g);const ring=M(new T.TorusGeometry(.94,.018,6,64),color,0,.05,0,g);ring.rotation.x=Math.PI/2;
    box(.65,.7,.1,ink,0,1.0,-.38,g);cylinder(.055,.5,metal,0,.3,-.25,g);box(.65,.08,.65,ink,0,.57,-.13,g);
    // Seated legs, shaped shoulders, waist and layered garments: ~3.5 head heights.
    [-1,1].forEach(s=>{rod([s*.17,.91,-.04],[s*.17,.57,.36],.095,trouser,character);rod([s*.17,.57,.36],[s*.17,.13,.39],.085,trouser,character);box(.19,.12,.33,ink,s*.17,.12,.48,character);});
    box(.49,.32,.3,trouser,0,.91,0,character);
    box(.61,.62,.32,shirt,0,1.32,0,character);
    const garment=(r.id==='tr'||r.id==='a1'||r.id==='pm'||r.id==='rk')?color:color;
    box(.68,.46,.36,garment,0,1.29,-.01,character);box(.73,.15,.34,garment,0,1.58,-.015,character);
    // Visible shirt front; knitted A2 and cardigan A5 intentionally have no lapels.
    if(!['a2','rv'].includes(r.id)){
      box(.16,.59,.02,shirt,0,1.34,.19,character);
      for(const s of[-1,1]){const lapel=box(.12,.32,.04,color,s*.13,1.48,.21,character);lapel.rotation.z=s*.3;}
    }else if(r.id==='rv'){
      box(.065,.56,.025,shirt,0,1.32,.195,character);for(let j=0;j<4;j++)ball(.017,ink,.055,1.13+j*.11,.22,character);
    }
    cylinder(.10,.14,skin,0,1.72,0,character);
    const arms=[-1,1].map(s=>{
      const sh=new T.Group();sh.position.set(s*.4,1.6,0);character.add(sh);
      rod([0,0,0],[0,-.30,0],.085,r.id==='tr'?shirt:color,sh);
      rod([0,-.30,0],[0,-.59,.03],.07,r.id==='tr'?skin:color,sh);
      if(r.id==='tr')cylinder(.09,.065,shirt,0,-.29,0,sh);
      ball(.08,skin,0,-.61,.04,sh);sh.rotation.z=s*.12;return sh;
    });
    const head=new T.Group();head.position.set(0,1.99,0);character.add(head);ball(.32,skin,0,0,0,head).scale.set(.91,1,1);
    [-1,1].forEach(s=>ball(.052,skin,s*.30,-.025,0,head).scale.set(.6,1,.8));
    const eyes=[-1,1].map(s=>{const eye=ball(.032,ink,s*.112,.015,.292,head);eye.scale.set(1,1.2,.5);ball(.009,white,.009,.014,.018,eye);return eye;});
    const brows=[-1,1].map(s=>box(.085,.015,.012,ink,s*.112,.10,.29,head));
    const mouth=box(.075,.015,.012,ink,0,-.12,.30,head);
    M(new T.SphereGeometry(.335,28,16,0,Math.PI*2,0,Math.PI*.51),hair,0,.015,-.015,head).rotation.x=-.12;
    for(const s of[-1,1])box(.06,.19,.17,hair,s*.276,-.025,-.045,head);
    if(r.style==='pony'){
      ball(.12,hair,.14,.43,-.13,head);rod([.14,.43,-.13],[.34,-.20,-.26],.105,hair,head);ball(.105,hair,.34,-.20,-.26,head);cylinder(.105,.04,gold,.14,.42,-.13,head);
    }
    if(r.style==='bun'){ball(.145,hair,0,.34,-.15,head);ball(.11,hair,0,.40,-.19,head);}
    if(r.glasses){
      for(const s of[-1,1]){
        if(r.glasses==='square'){for(const y of[-.054,.065])box(.18,.014,.014,glass,s*.114,y,.323,head);for(const x of[-.087,.087])box(.014,.12,.014,glass,s*.114+x,.005,.323,head);}
        else M(new T.TorusGeometry(.071,r.glasses==='thin'?.007:.011,8,30),glass,s*.112,.01,.322,head);
      }rod([-.035,.02,.325],[.035,.02,.325],.008,glass,head);
    }
    if(r.headset){
      M(new T.TorusGeometry(.35,.022,8,32,Math.PI),ink,0,0,0,head);
      for(const s of[-1,1])ball(.075,ink,s*.32,-.015,0,head).scale.set(.55,1,1);
      rod([-.32,-.04,.03],[-.14,-.14,.30],.012,ink,head);ball(.024,ink,-.14,-.14,.30,head);
    }
    if(r.id==='a2'){
      M(new T.TorusGeometry(.14,.025,8,30,Math.PI),ink,0,1.69,.1,character).rotation.x=Math.PI/2;
      for(const s of[-1,1])ball(.06,ink,s*.16,1.67,.06,character);
      box(.11,.15,.024,gold,.20,1.29,.21,character);
    }
    if(r.id==='pm'){M(new T.ConeGeometry(.065,.27,4),ink,0,1.44,.23,character).rotation.z=Math.PI;head.rotation.x=.16;}
    if(r.id==='a1')ball(.028,gold,.21,1.5,.22,character);
    if(r.beard){M(new T.SphereGeometry(.322,24,12,Math.PI/2-1.05,2.1,Math.PI*.63,Math.PI*.30),hair,0,0,0,head);box(.12,.032,.025,hair,0,-.07,.3,head);shield(gold,character,.22,1.47,.2,.28);}
    const desk=new T.Group();desk.position.set(0,0,.66);g.add(desk);
    box(1.85,.075,.72,deskMat,0,.89,0,desk);for(const s of[-1,1])box(.075,.85,.62,darkDesk,s*.83,.45,0,desk);
    box(1.75,.38,.055,darkDesk,0,.66,.29,desk);box(1.75,.025,.055,color,0,.82,.325,desk);box(.48,.025,.19,ink,0,.948,-.02,desk);
    const screens=[];
    if(r.id==='a2'){for(let k=0;k<3;k++)screens.push(screen({...r,screenKind:['themes','emotion','limitup'][k],color:k===2?'#d32f4b':r.color},desk,(k-1)*.5,1.19,.11,.42,.29));}
    else if(r.id==='tr'){for(const s of[-1,1])screens.push(screen({...r,screenKind:s<0?'price':'conditions'},desk,s*.36,1.23,.10,.61,.38));}
    else screens.push(screen(r,desk,-.12,1.21,.11,.72,.40));
    let prop=null,emergency=null,bookCover=null;
    function hingedBook(x,y,z,colorMat){
      box(.34,.04,.25,white,x,y,z,desk);const pivot=new T.Group();pivot.position.set(x,y+.025,z-.12);desk.add(pivot);box(.35,.018,.25,colorMat,0,0,.12,pivot);return pivot;
    }
    if(r.id==='a1'){
      cylinder(.05,.16,metal,.65,.99,-.04,desk);prop=new T.Group();prop.position.set(.65,1.20,-.04);desk.add(prop);
      ball(.18,material(0x6d4fd8,{wireframe:true}),0,0,0,prop);const orbit=M(new T.TorusGeometry(.22,.012,6,40),gold,0,0,0,prop);orbit.rotation.x=.4;
      for(let j=0;j<3;j++)box(.33,.04,.22,white,-.65,.96+j*.043,-.05,desk);
      bookCover=hingedBook(-.65,1.08,-.05,color);
    }
    if(r.id==='pm'){
      cylinder(.075,.17,white,.66,1.01,-.08,desk);M(new T.TorusGeometry(.045,.014,8,18),white,.735,1.02,-.08,desk);
      const dial=cylinder(.13,.04,metal,.60,1.04,.23,desk);dial.rotation.x=Math.PI/2;rod([.60,1.04,.259],[.65,1.11,.259],.009,ink,desk);
      bookCover=hingedBook(-.67,.98,.23,color);
    }
    if(r.id==='tr'){
      cylinder(.11,.05,ink,.72,.96,-.12,desk);emergency=cylinder(.085,.055,material(0x9d3542),.72,1.01,-.12,desk);
    }
    if(r.id==='rk'){prop=shield(color,desk,.68,1.13,-.02,.85);cylinder(.12,.04,metal,.68,.96,-.02,desk);}
    if(r.id==='rv'){
      for(const [j,c]of[0xc2454d,0x356faf,0xd7a12b].entries()){const book=box(.35,.055,.25,material(c),.65,.97+j*.06,-.02,desk);book.rotation.y=(j-1)*.15;}
      bookCover=hingedBook(.65,1.105,-.02,gold);
      rod([-.58,.95,-.12],[-.47,1.15,-.12],.012,ink,desk);
    }
    const lamp=material(0xfff0c2,{emissive:0xf0b429,emissiveIntensity:0});ball(.035,lamp,-.78,1.02,-.18,desk);
    const dome=M(new T.SphereGeometry(1.08,32,16,0,Math.PI*2,0,Math.PI/2),material(0xe0a100,{transparent:true,opacity:.14,side:T.DoubleSide}),0,.12,.05,g);dome.scale.y=2.25;dome.visible=false;
    const zcv=document.createElement('canvas');zcv.width=128;zcv.height=64;const zctx=zcv.getContext('2d');zctx.font='600 36px sans-serif';zctx.fillStyle='#586372';zctx.fillText('z z',8,46);const ztex=new T.CanvasTexture(zcv);textures.push(ztex);const zmat=new T.MeshBasicMaterial({map:ztex,transparent:true,depthWrite:false});materials.push(zmat);const zz=M(new T.PlaneGeometry(.55,.275),zmat,.30,2.57,.12,g);zz.visible=false;
    stations.set(r.id,{g,character,head,arms,eyes,brows,mouth,ring,prop,emergency,dome,lamp,bookCover,zz,state:'idle',since:0,phase:i*.9,index:i});
  }
  // Neutral back-wall screens communicate missing data, never invented market numbers.
  for(const s of[-1,1]){const p=screen({short:s<0?'市场状态':'任务接力',color:'#a1adbe'},environment,s*5,2.8,-2.10,3.8,1.3);p.castShadow=false;}
  const fillLamp=new T.PointLight(0xffe6aa,preview?.25:.45,12);fillLamp.position.set(0,3,1);scene.add(fillLamp);
  function theme(){const dark=document.documentElement.dataset.theme==='dark';scene.background=new T.Color(view==='silhouette'?0xf1f2f4:dark?0x192030:0xe9edf4);planeMat.color.set(dark?0x242d3d:0xdfe5ee);deskMat.color.set(dark?0x303a4b:0xfafbfc);darkDesk.color.set(dark?0x252f41:0xd5dce7);wallMat.color.set(dark?0x202b3e:0xcbd5e3);hemi.intensity=dark?.65:.8;key.intensity=dark?.8:.9;renderer.render(scene,camera);}
  const observer=new MutationObserver(theme);observer.observe(document.documentElement,{attributes:true,attributeFilter:['data-theme']});theme();
  function resize(){const w=Math.max(1,host.clientWidth),h=Math.max(1,host.clientHeight);renderer.setSize(w,h,false);camera.aspect=w/h;camera.updateProjectionMatrix();render(performance.now());}
  function render(now){
    const dt=Math.min(.05,(now-last)/1000||0);last=now;const t=paused||reduce?0.75:now/1000;
    const mobile=host.clientWidth<720;
    for(const s of stations.values()){
      if(mobile){s.g.position.set((s.index%3-1)*2.72,0,s.index<3?-1.7:1.7);s.g.rotation.y=0;}
      else{s.g.position.set((s.index-2.5)*2.72,0,.055*(s.index-2.5)**2);s.g.rotation.y=-s.g.position.x*.018;}
    }
    const chosen=stations.get(focus);
    if(chosen){camera.position.set(chosen.g.position.x,3.4,chosen.g.position.z+6.6);camera.lookAt(chosen.g.position.x,1.6,chosen.g.position.z+.25);}
    else if(mobile){camera.position.set(0,9.8,18);camera.lookAt(0,1.3,0);}
    else{const d=Math.max(12.5,8.75/(Math.tan(14.5*Math.PI/180)*camera.aspect));camera.position.set(0,1.35+d*.20,d);camera.lookAt(0,1.35,0);}
    for(const [id,s]of stations){
      let st=s.state;if(st==='happy'&&now-s.since>1500&&!paused){st=s.state='work';}
      s.g.visible=focus==='all'||focus===id;
      s.character.position.y=st==='happy'?.08:0;
      const a=Math.sin(t*(id==='a2'?19:12)+s.phase)*.09;
      let left=[0,-.12],right=[0,.12];
      if(st==='work'){
        left=[-.8+a,-.08];right=[-.8-a,.08];
        if(id==='a1')right=[-.95,.45];if(id==='pm'){left=[-.5,-.16];right=[-.85,.2];}
        if(id==='rk'){left=[-.35,-.4];right=[-.95,-.05+a*.4];}if(id==='rv')right=[-.7+a*.4,.2];
      }else if(st==='talk'){left=[-.30,-.14];right=id==='a1'?[-2.2,.25]:[-.9,.38+(id==='pm'?0:a)];}
      else if(st==='stop'){left=[-.2,-.1];right=id==='rk'?[-1.3,.30]:[-.25,.30];}
      else if(st==='rest'){left=[-.45,-.12];right=[-.45,.12];}
      else if(st==='happy'){left=[-.3,-.16];right=[-.7,.6];}
      else if(id==='rk'){left=[-.7,.7];right=[-.7,-.7];}
      for(const [i,angles]of[left,right].entries()){s.arms[i].rotation.x=angles[0];s.arms[i].rotation.z=angles[1];}
      s.character.rotation.x=st==='rest'?-.08:st==='work'?.035:0;
      s.head.rotation.x=(id==='pm'?.16:0)+(st==='work'?.08:st==='rest'?.11:0);
      s.head.rotation.y=st==='stop'&&id!=='rk'&&id!=='tr'?Math.sign(stations.get('rk').g.position.x-s.g.position.x)*.4:st==='talk'?.06:Math.sin(t*.6+s.phase)*.035;
      const blink=st==='rest'?.1:Math.sin(t*(.8+s.phase*.08))>.99?.15:1;
      s.eyes.forEach(eye=>eye.scale.y=1.2*blink);
      s.brows.forEach((b,i)=>b.rotation.z=st==='stop'?(i?-.2:.2):0);
      s.mouth.scale.y=st==='talk'?2+Math.abs(Math.sin(t*9))*2:1;
      s.dome.visible=id==='rk'&&st==='stop';if(s.emergency)s.emergency.material.emissive.set(st==='stop'?0xe33542:0x000000);
      s.zz.visible=st==='rest';
      if(s.bookCover){
        if(id==='rv')s.bookCover.rotation.x=st==='work'?-.25-Math.abs(Math.sin(t*1.7))*.65:st==='talk'?0:-.12;
        else if(id==='pm')s.bookCover.rotation.x=st==='work'?-.3-Math.abs(Math.sin(t*1.35))*.6:0;
        else if(id==='a1')s.bookCover.rotation.x=st==='idle'?-.15-Math.abs(Math.sin(t*.65))*.5:0;
      }
      s.lamp.emissiveIntensity=st==='work'||st==='talk'?0.65:0;
      if(s.prop&&id==='a1'&&st==='work'&&!paused&&!reduce)s.prop.rotation.y+=dt*.9;
    }
    renderer.render(scene,camera);
    host.setAttribute('aria-label',ROLES.filter(r=>focus==='all'||focus===r.id).map(r=>`${r.name}：${stations.get(r.id).state}`).join('；'));
  }
  function armHappyTimeout(id){
    clearTimeout(happyTimers.get(id));
    const timer=setTimeout(()=>{happyTimers.delete(id);if(disposed)return;const s=stations.get(id);if(s.state==='happy'){s.state='work';render(performance.now());}},1500);
    happyTimers.set(id,timer);
  }
  function loop(now){if(disposed)return;raf=requestAnimationFrame(loop);render(now);}
  function sync(){cancelAnimationFrame(raf);raf=0;if(!disposed&&visible&&!document.hidden&&!paused&&!reduce)raf=requestAnimationFrame(loop);else if(!disposed)render(performance.now());}
  const intersection=new IntersectionObserver(entries=>{visible=entries[0].isIntersecting;sync();});intersection.observe(host);
  const visibility=()=>sync();document.addEventListener('visibilitychange',visibility);
  const reductionChanged=()=>{reduce=reduceQuery.matches;sync();};reduceQuery.addEventListener('change',reductionChanged);
  const ro=new ResizeObserver(resize);ro.observe(host);resize();sync();
  renderer.domElement.addEventListener('webglcontextlost',event=>{event.preventDefault();cancelAnimationFrame(raf);onUnavailable('WEBGL_CONTEXT_LOST');});
  return {
    focus(id){if(id!=='all'&&!stations.has(id))return;focus=id;render(performance.now());},
    setRole(id,state){if(!stations.has(id)||!STATES.includes(state))return;clearTimeout(happyTimers.get(id));happyTimers.delete(id);const s=stations.get(id);s.state=state;s.since=performance.now();if(state==='happy'&&(!preview||!paused))armHappyTimeout(id);render(performance.now());},
    setAll(state){for(const r of ROLES)this.setRole(r.id,state);},
    pause(value){paused=Boolean(value);if(!paused)for(const [id,s]of stations)if(s.state==='happy')armHappyTimeout(id);sync();},
    setView(mode){view=mode;scene.overrideMaterial=mode==='silhouette'?shaderSilhouette:null;environment.visible=mode!=='silhouette';theme();render(performance.now());},
    dispose(){disposed=true;cancelAnimationFrame(raf);for(const timer of happyTimers.values())clearTimeout(timer);happyTimers.clear();ro.disconnect();intersection.disconnect();observer.disconnect();reduceQuery.removeEventListener('change',reductionChanged);document.removeEventListener('visibilitychange',visibility);geometries.forEach(g=>g.dispose());materials.forEach(m=>m.dispose());textures.forEach(t=>t.dispose());renderer.dispose();renderer.domElement.remove();}
  };
}
