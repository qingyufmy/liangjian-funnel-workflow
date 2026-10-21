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
  const material=(color,extra={})=>{const v=new T.MeshStandardMaterial({color,roughness:.92,...extra});materials.push(v);return v;};
  const skin=material(0xD8B699),shirt=material(0xE9EDF2),ink=material(0x0C1119),metal=material(0xA7B2C6,{metalness:.5,roughness:.4});
  const white=material(0xE9EDF2),trouser=material(0x363B44),glass=material(0x3B4350),gold=material(0xF0B429);
  const environment=new T.Group();scene.add(environment);
  const M=(geo,mat,x,y,z,parent)=>{geometries.push(geo);const mesh=new T.Mesh(geo,mat);mesh.position.set(x,y,z);mesh.castShadow=true;mesh.receiveShadow=true;parent.add(mesh);return mesh;};
  const box=(w,h,d,mat,x,y,z,parent)=>M(new T.BoxGeometry(w,h,d),mat,x,y,z,parent);
  const ball=(r,mat,x,y,z,parent)=>M(new T.SphereGeometry(r,24,16),mat,x,y,z,parent);
  const cylinder=(r,h,mat,x,y,z,parent)=>M(new T.CylinderGeometry(r,r,h,20),mat,x,y,z,parent);
  const rod=(a,b,r,mat,parent)=>{const va=new T.Vector3(...a),vb=new T.Vector3(...b),dir=vb.clone().sub(va);const p=M(new T.CylinderGeometry(r,r,dir.length(),10),mat,...va.clone().add(vb).multiplyScalar(.5).toArray(),parent);p.quaternion.setFromUnitVectors(new T.Vector3(0,1,0),dir.normalize());return p;};
  const planeMat=material(0x141B26),deskMat=material(0x2A3444),darkDesk=material(0x1F2938);
  box(19,.13,6.4,planeMat,0,-.1,.1,environment);
  const wallMat=material(0x141B26);
  box(19,4.5,.13,wallMat,0,2,-2.25,environment);
  const hemi=new T.HemisphereLight(0xeaf2ff,0xb6bcc8,.8);scene.add(hemi);
  const key=new T.DirectionalLight(0xffecd4,1.05);key.position.set(-4,8,7);key.castShadow=true;key.shadow.mapSize.set(2048,1024);key.shadow.radius=4;Object.assign(key.shadow.camera,{left:-10,right:10,top:5,bottom:-5});key.shadow.camera.updateProjectionMatrix();key.shadow.bias=-.001;scene.add(key);
  const rim=new T.DirectionalLight(0xaec9ff,.35);rim.position.set(4,5,-3);scene.add(rim);
  const stations=new Map(),happyTimers=new Map();let focus='all',paused=preview,view='color',last=0,raf=0,visible=true,disposed=false;
  const stageContext={market:null,currentRole:null};
  const stationPosition=i=>new T.Vector3((i-2.5)*2.25,0,-.95+.22*(i-2.5)**2);
  const relayPoints=ROLES.map((_,i)=>{const p=stationPosition(i);return new T.Vector3(p.x,.07,p.z+1.2);});
  const relayPath=new T.CatmullRomCurve3(relayPoints,false,'centripetal');
  const relayBaseMat=material(0x2C3747),relaySegments=[];
  for(let i=0;i<relayPoints.length-1;i++){
    const section=new T.Curve();section.getPoint=t=>relayPath.getPoint((i+t)/(relayPoints.length-1));
    relaySegments.push(M(new T.TubeGeometry(section,24,.015,6,false),relayBaseMat,0,0,0,environment));
  }
  const relayDot=ball(.085,material(0xF0B429,{emissive:0xF0B429,emissiveIntensity:.2}),0,.10,1.2,environment);relayDot.visible=false;
  const glowMaterial=new T.MeshBasicMaterial({color:0xF0B429,transparent:true,opacity:.18,depthWrite:false});materials.push(glowMaterial);
  const relayGlow=M(new T.SphereGeometry(.158,24,16),glowMaterial,0,0,0,relayDot);relayGlow.scale.setScalar(1);relayGlow.castShadow=false;
  let relayMotion=null,relayRaf=0;
  // Architectural seams / soft top lighting, not decorative financial data.
  const mullion=material(0x2A3444,{metalness:.45,roughness:.6});
  for(let x=-9;x<=9;x+=3)box(.025,4.4,.025,mullion,x,2,-2.15,environment);
  box(18,.025,.025,mullion,0,1.5,-2.14,environment);
  const stripMat=material(0xfff4dc,{emissive:0xfff4dc,emissiveIntensity:.7});
  for(const x of[-5,0,5])box(3.3,.025,.08,stripMat,x,3.5,-.8,environment);
  const reduceQuery=matchMedia('(prefers-reduced-motion: reduce)');let reduce=reduceQuery.matches;
  const shaderSilhouette=new T.MeshBasicMaterial({color:0x12161D});materials.push(shaderSilhouette);
  function textScreen(role,width=320,height=170){
    const cv=document.createElement('canvas');cv.width=width;cv.height=height;const c=cv.getContext('2d');
    c.fillStyle='#0C1119';c.fillRect(0,0,width,height);c.fillStyle=role.color;c.fillRect(0,0,width,5);
    c.fillStyle='#E8ECF2';c.font='600 20px "Microsoft YaHei",sans-serif';c.fillText(role.short,16,37);
    c.font='14px "Microsoft YaHei",sans-serif';c.fillStyle='#9AA6B8';c.fillText('原始记录待接入',16,59);
    const line=(label,value,y)=>{c.fillStyle='#9AA6B8';c.fillText(label,16,y);c.fillStyle='#E8ECF2';c.textAlign='right';c.fillText(value,width-16,y);c.textAlign='left';};
    c.strokeStyle='#2A3546';
    if(role.id==='a1'){
      for(const [i,label]of['财报项目','营收 / 利润','现金流','财务初筛'].entries()){
        const x=16,y=72+i*22;c.strokeRect(x,y,width-32,20);c.fillStyle='#9AA6B8';c.fillText(label,x+7,y+14);c.textAlign='right';c.fillText('待源',width-24,y+14);c.textAlign='left';}
    }else if(role.id==='a2'){
      const labels=role.screenKind==='emotion'?['涨停 —','破板 —','连板 —','跌停 —']:role.screenKind==='limitup'?['涨停池 · 缺原件','暂无可证列表','轮动速度 —']:['题材热度榜','强度 / 阶段均待源','不推演当日题材'];
      labels.forEach((label,i)=>{line(label,'',82+i*28);c.strokeRect(16,89+i*28,width-32,5);});
    }else if(role.id==='pm'){
      c.beginPath();c.arc(63,113,31,0,Math.PI*2);c.stroke();c.fillStyle='#9AA6B8';c.font='14px sans-serif';c.fillText('待源',47,118);line('使用率','待源',84);line('槽位','待源',112);line('市场','待源',140);
    }else if(role.id==='tr'){
      if(role.screenKind==='price'){c.beginPath();c.moveTo(25,80);c.lineTo(25,145);c.lineTo(295,145);c.stroke();line('分时 / K线','待报价',94);line('缺源，不画走势','',125);}
      else{line('现价 / 回踩区距离','—',86);line('入场条件','缺原件',112);line('浮动盈亏','—',138);}
    }else if(role.id==='rk'){
      ['单笔预算','现金底线','新开限制','同簇上限','截止时刻'].forEach((label,i)=>{c.beginPath();c.arc(22,75+i*18,3,0,Math.PI*2);c.stroke();c.fillStyle='#9AA6B8';c.fillText(label,34,80+i*18);c.textAlign='right';c.fillText('待源',width-16,80+i*18);c.textAlign='left';});
    }else if(role.id==='rv'){
      for(let row=0;row<2;row++)for(let col=0;col<6;col++)c.strokeRect(16+col*24,77+row*23,18,17);
      line('结果格 · 尚无来源','',139);line('复盘记录','待源',158);
    }else{line('实时来源','未连接',94);line('不补造行情或任务','',124);}
    const tex=new T.CanvasTexture(cv);textures.push(tex);const mat=new T.MeshBasicMaterial({map:tex});materials.push(mat);return mat;
  }
  function shield(mat,parent,x,y,z,scale=1){
    const shape=new T.Shape();shape.moveTo(0,.2);shape.lineTo(.17,.12);shape.lineTo(.13,-.09);shape.lineTo(0,-.23);shape.lineTo(-.13,-.09);shape.lineTo(-.17,.12);shape.closePath();
    const m=M(new T.ExtrudeGeometry(shape,{depth:.045,bevelEnabled:false}),mat,x,y,z,parent);m.scale.setScalar(scale);return m;
  }
  function screen(role,parent,x,y,z,w=.65,h=.42){
    rod([x,y-.25,z],[x,y,z],.014,metal,parent);box(w*.35,.018,.13,metal,x,y-.25,z,parent);box(w+.018,h+.018,.028,ink,x,y,z,parent);
    const m=M(new T.PlaneGeometry(w,h),textScreen(role),x,y,z+.016,parent);return m;
  }
  for(const [i,r]of ROLES.entries()){
    const g=new T.Group();g.position.copy(stationPosition(i));g.rotation.y=(2.5-i)*.042;scene.add(g);
    const color=material(r.hex),hair=material(r.hair),character=new T.Group();g.add(character);
    const suit=material({a1:0x2E3B4E,a2:0x2F4F55,pm:0x1F2A44,rk:0x3A3F47,tr:0x363B44,rv:0x2E4A3F}[r.id]);
    cylinder(1.02,.035,planeMat,0,0,0,g).scale.z=1.3;const ring=M(new T.TorusGeometry(.96,.014,6,64),color,0,.045,0,g);ring.rotation.x=Math.PI/2;ring.scale.y=1.3;
    box(.65,.7,.1,ink,0,1.0,-.38,g);cylinder(.055,.5,metal,0,.3,-.25,g);box(.65,.08,.65,ink,0,.57,-.13,g);
    // Seated legs, shaped shoulders, waist and layered garments: ~3.5 head heights.
    [-1,1].forEach(s=>{rod([s*.17,.91,-.04],[s*.17,.57,.36],.095,trouser,character);rod([s*.17,.57,.36],[s*.17,.13,.39],.085,trouser,character);box(.19,.12,.33,ink,s*.17,.12,.48,character);});
    box(.49,.32,.3,trouser,0,.91,0,character);
    const garment=suit;
    const torso=new T.Shape();
    torso.moveTo(-.30,1.015);torso.lineTo(.30,1.015);torso.lineTo(.40,1.60);
    torso.quadraticCurveTo(.43,1.70,.34,1.715);torso.lineTo(-.34,1.715);
    torso.quadraticCurveTo(-.43,1.70,-.40,1.60);torso.closePath();
    M(new T.ExtrudeGeometry(torso,{depth:.30,bevelEnabled:true,bevelThickness:.018,bevelSize:.018,bevelSegments:3,steps:1}),garment,0,0,-.16,character);
    // Visible shirt front; knitted A2 and cardigan A5 intentionally have no lapels.
    if(!['a2','rv'].includes(r.id)){
      box(.16,.59,.02,shirt,0,1.34,.19,character);
      for(const s of[-1,1]){const lapel=box(.12,.32,.04,suit,s*.13,1.48,.21,character);lapel.rotation.z=s*.3;}
    }else if(r.id==='rv'){
      box(.065,.56,.025,shirt,0,1.32,.195,character);for(let j=0;j<4;j++)ball(.017,ink,.055,1.13+j*.11,.22,character);
    }
    cylinder(.08,.14,skin,0,1.78,0,character);
    for(const s of[-1,1])rod([s*.07,1.63,.205],[s*.03,1.31,.205],.008,color,character);
    box(.065,.095,.016,color,.03,1.29,.21,character);
    const arms=[-1,1].map(s=>{
      const sh=new T.Group();sh.position.set(s*.43,1.65,0);character.add(sh);
      ball(.09,r.id==='tr'?shirt:suit,0,0,0,sh);
      rod([0,0,0],[0,-.44,.08],.08,r.id==='tr'?shirt:suit,sh);
      ball(.073,r.id==='tr'?skin:suit,0,-.44,.08,sh);
      if(r.id==='tr')cylinder(.085,.055,shirt,0,-.415,.075,sh);
      const forearm=new T.Group();forearm.position.set(0,-.44,.08);sh.add(forearm);
      rod([0,0,0],[0,-.46,.02],.062,r.id==='tr'?skin:suit,forearm);
      ball(.073,skin,0,-.48,.035,forearm);forearm.rotation.x=-.8;
      sh.userData.forearm=forearm;sh.rotation.z=-s*.30;return sh;
    });
    const head=new T.Group();head.position.set(0,2.02,0);head.scale.setScalar(.75);character.add(head);ball(.32,skin,0,0,0,head).scale.set(.91,1,1);
    [-1,1].forEach(s=>ball(.052,skin,s*.30,-.025,0,head).scale.set(.6,1,.8));
    const eyes=[-1,1].map(s=>{const eye=ball(.023,ink,s*.112,.015,.292,head);eye.scale.set(1,.45,.4);return eye;});
    const brows=[-1,1].map(s=>box(.085,.015,.012,ink,s*.112,.10,.29,head));
    const mouth=box(.075,.015,.012,ink,0,-.12,.30,head);
    if(r.style==='flat')box(.54,.13,.43,hair,0,.24,-.025,head);
    else M(new T.SphereGeometry(.335,28,16,0,Math.PI*2,0,Math.PI*.51),hair,0,.015,-.015,head).rotation.x=-.12;
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
      rod([-.32,-.04,.03],[-.43,-.15,.38],.016,ink,head);rod([-.43,-.15,.38],[-.13,-.15,.43],.016,ink,head);ball(.03,ink,-.13,-.15,.43,head);
    }
    if(r.id==='a2'){
      M(new T.TorusGeometry(.14,.025,8,30,Math.PI),ink,0,1.69,.1,character).rotation.x=Math.PI/2;
      for(const s of[-1,1])ball(.06,ink,s*.16,1.67,.06,character);
      box(.11,.15,.024,gold,.20,1.29,.21,character);
    }
    if(r.id==='pm'){M(new T.ConeGeometry(.045,.27,4),color,0,1.44,.23,character).rotation.z=Math.PI;head.rotation.x=.21;}
    if(r.id==='a1')ball(.028,gold,.21,1.5,.22,character);
    if(r.beard){M(new T.SphereGeometry(.322,24,12,Math.PI/2-1.05,2.1,Math.PI*.63,Math.PI*.30),hair,0,0,0,head);box(.12,.032,.025,hair,0,-.07,.3,head);shield(gold,character,.22,1.47,.2,.28);}
    const desk=new T.Group();desk.position.set(0,0,.66);g.add(desk);
    box(1.95,.035,.85,metal,0,.895,0,desk);box(1.94,.045,.84,deskMat,0,.925,0,desk);for(const s of[-1,1])box(.035,.85,.62,metal,s*.85,.45,0,desk);
    box(1.75,.25,.025,darkDesk,0,.69,.29,desk);box(.48,.018,.19,ink,0,.958,-.04,desk);
    const screens=[];
    if(r.id==='a2'){for(let k=0;k<3;k++)screens.push(screen({...r,screenKind:['themes','emotion','limitup'][k],color:r.color},desk,(k-1)*.5,1.19,.11,.42,.29));}
    else if(r.id==='tr'){for(const s of[-1,1])screens.push(screen({...r,screenKind:s<0?'price':'conditions'},desk,s*.36,1.23,.10,.61,.38));}
    else screens.push(screen(r,desk,-.12,1.21,.11,.72,.40));
    let prop=null,emergency=null,bookCover=null;
    function hingedBook(x,y,z,colorMat){
      box(.34,.04,.25,white,x,y,z,desk);const pivot=new T.Group();pivot.position.set(x,y+.025,z-.12);desk.add(pivot);box(.35,.018,.25,colorMat,0,0,.12,pivot);return pivot;
    }
    if(r.id==='a1'){
      cylinder(.065,.06,metal,.92,.978,.03,desk);prop=new T.Group();prop.position.set(.92,1.57,.03);desk.add(prop);
      rod([.92,1.008,.03],[.92,1.36,.03],.014,metal,desk);ball(.21,material(0xA7B2C6,{wireframe:true}),0,0,0,prop);const orbit=M(new T.TorusGeometry(.255,.012,6,40),metal,0,0,0,prop);orbit.rotation.x=.4;
      for(let j=0;j<3;j++)box(.33,.04,.22,white,-.65,.96+j*.043,-.05,desk);
      bookCover=hingedBook(-.65,1.08,-.05,color);
    }
    if(r.id==='pm'){
      M(new T.CylinderGeometry(.075,.075,.21,24,1,true),white,.88,1.055,-.29,desk);
      const cupRim=M(new T.TorusGeometry(.075,.009,8,24),white,.88,1.160,-.29,desk);cupRim.rotation.x=Math.PI/2;
      cylinder(.075,.012,white,.88,.956,-.29,desk);M(new T.TorusGeometry(.045,.012,8,18),white,.99,1.065,-.29,desk);
      const dial=cylinder(.13,.04,metal,.60,1.04,.23,desk);dial.rotation.x=Math.PI/2;rod([.60,1.04,.259],[.65,1.11,.259],.009,ink,desk);
      bookCover=hingedBook(-.67,.98,.23,color);
    }
    if(r.id==='tr'){
      cylinder(.12,.065,metal,.76,.985,.27,desk);emergency=cylinder(.10,.075,material(0x9E2A2A),.76,1.055,.27,desk);
    }
    if(r.id==='rk'){prop=shield(metal,desk,.68,1.25,.12,1.28);cylinder(.15,.05,metal,.68,.98,.12,desk);rod([.68,.99,.12],[.68,1.11,.12],.028,metal,desk);}
    if(r.id==='rv'){
      for(const [j,c]of[0xD23B3B,0x5C8BE6,0xF0B429].entries()){const book=box(.35,.055,.25,material(c),.65,.97+j*.06,-.02,desk);book.rotation.y=(j-1)*.15;}
      bookCover=hingedBook(.65,1.105,-.02,gold);
      rod([-.58,.95,-.12],[-.47,1.15,-.12],.012,ink,desk);
    }
    const lamp=material(0xE9EDF2,{emissive:0xE9EDF2,emissiveIntensity:0});ball(.035,lamp,-.78,1.02,-.18,desk);
    const dome=M(new T.SphereGeometry(1.08,32,16,0,Math.PI*2,0,Math.PI/2),material(0xA7B2C6,{transparent:true,opacity:.14,side:T.DoubleSide}),0,.12,.05,g);dome.scale.y=2.25;dome.visible=false;
    const zcv=document.createElement('canvas');zcv.width=128;zcv.height=64;const zctx=zcv.getContext('2d');zctx.font='600 36px sans-serif';zctx.fillStyle='#9AA6B8';zctx.fillText('z z',8,46);const ztex=new T.CanvasTexture(zcv);textures.push(ztex);const zmat=new T.MeshBasicMaterial({map:ztex,transparent:true,depthWrite:false});materials.push(zmat);const zz=M(new T.PlaneGeometry(.55,.275),zmat,.30,2.57,.12,g);zz.visible=false;
    stations.set(r.id,{g,character,head,arms,eyes,brows,mouth,ring,prop,emergency,dome,lamp,bookCover,zz,state:'idle',since:0,phase:i*.9,index:i});
  }
  // Market and relay text live in the HTML room tags / footer, per HOME_REFERENCE.
  const fillLamp=new T.PointLight(0xffe6aa,preview?.25:.45,12);fillLamp.position.set(0,3,1);scene.add(fillLamp);
  const wallCanvas=document.createElement('canvas');wallCanvas.width=8;wallCanvas.height=256;
  const wallTexture=new T.CanvasTexture(wallCanvas);textures.push(wallTexture);wallMat.map=wallTexture;
  function theme(){scene.background=new T.Color(view==='silhouette'?0xF3F4F6:0x141B26);planeMat.color.set(0x141B26);deskMat.color.set(0x2A3444);darkDesk.color.set(0x1F2938);wallMat.color.set(0xFFFFFF);const c=wallCanvas.getContext('2d'),gradient=c.createLinearGradient(0,0,0,256);gradient.addColorStop(0,'#223047');gradient.addColorStop(1,'#141B26');c.fillStyle=gradient;c.fillRect(0,0,8,256);wallTexture.needsUpdate=true;relayBaseMat.color.set(0x2C3747);hemi.intensity=.50;key.intensity=.90;renderer.render(scene,camera);}
  const observer=new MutationObserver(theme);observer.observe(document.documentElement,{attributes:true,attributeFilter:['data-theme']});theme();
  function resize(){const w=Math.max(1,host.clientWidth<720&&focus==='all'?960:host.clientWidth),h=Math.max(1,host.clientHeight);renderer.setSize(w,h,false);renderer.domElement.style.width=`${w}px`;renderer.domElement.style.height=`${h}px`;renderer.domElement.style.maxWidth='none';camera.aspect=w/h;camera.updateProjectionMatrix();render(performance.now());}
  function render(now){
    const dt=Math.min(.05,(now-last)/1000||0);last=now;const t=paused||reduce?0.75:now/1000;
    for(const s of stations.values()){
      s.g.position.copy(stationPosition(s.index));s.g.rotation.y=(2.5-s.index)*.042;
    }
    const chosen=stations.get(focus);
    if(chosen){camera.position.set(chosen.g.position.x,3.4,chosen.g.position.z+6.6);camera.lookAt(chosen.g.position.x,1.6,chosen.g.position.z+.25);}
    else{const d=Math.max(host.clientHeight<=340?7.8:7.2,7.1/(Math.tan(14.5*Math.PI/180)*camera.aspect));camera.position.set(0,1.40+d*.325,d);camera.lookAt(0,1.40,.2);}
    if(relayMotion){
      const progress=reduce?1:Math.min(1,(now-relayMotion.start)/720),ease=1-Math.pow(1-progress,3);
      const position=relayMotion.from+(relayMotion.to-relayMotion.from)*ease;
      relayDot.position.copy(relayPath.getPoint(position/(ROLES.length-1)));relayDot.position.y=.10;
      if(progress===1){
        const low=Math.min(relayMotion.from,relayMotion.to),high=Math.max(relayMotion.from,relayMotion.to);
        for(let i=low;i<high;i++){const mat=material(0xF0B429,{transparent:true,opacity:.85,emissive:0xF0B429,emissiveIntensity:.10});relaySegments[i].material=mat;}
        relayMotion=null;
      }
    }
    for(const [id,s]of stations){
      let st=s.state;if(st==='happy'&&now-s.since>1500&&!paused){st=s.state='work';}
      s.g.visible=focus==='all'||focus===id;
      s.character.position.y=st==='happy'?.08:0;
      const a=Math.sin(t*(id==='a2'?19:12)+s.phase)*.09;
      let left=[-.20,.30],right=[-.20,-.30];
      if(st==='work'){
        left=[-.20+a*.08,.30];right=[-.20-a*.08,-.30];
        if(id==='a1')right=[-.95,.45];if(id==='pm'){left=[-.5,-.16];right=[-.85,.2];}
        if(id==='rk'){left=[-.35,-.4];right=[-.95,-.05+a*.4];}if(id==='rv')right=[-.7+a*.4,.2];
      }else if(st==='talk'){left=[-.30,-.14];right=id==='a1'?[-2.2,.25]:[-.9,.38+(id==='pm'?0:a)];}
      else if(st==='stop'){left=[-.2,-.1];right=id==='rk'?[-1.3,.30]:[-.25,.30];}
      else if(st==='rest'){left=[-.20,.30];right=[-.20,-.30];}
      else if(st==='happy'){left=[-.3,-.16];right=[-.7,.6];}
      else if(id==='rk'){left=[-.55,-.22];right=[-.55,.22];}
      for(const [i,angles]of[left,right].entries()){
        const arm=s.arms[i];arm.rotation.x=angles[0];arm.rotation.z=angles[1];
        // Separate elbow joint: seated forearms meet the desk instead of a rigid raised limb.
        arm.userData.forearm.rotation.x=id==='rk'&&st==='idle'?-.80:st==='work'?-.92+(i?a:-a)*.18:st==='talk'?-1.0:st==='stop'?-.75:-.80;
        arm.userData.forearm.rotation.z=id==='rk'&&st==='idle'?(i?-1.7:1.7):0;
      }
      s.character.rotation.x=st==='rest'?-.08:st==='work'?.035:0;
      s.head.rotation.x=(id==='pm'?.26:0)+(st==='work'?.045:st==='rest'?.11:0);
      s.head.rotation.y=st==='stop'&&id!=='rk'&&id!=='tr'?Math.sign(stations.get('rk').g.position.x-s.g.position.x)*.4:st==='talk'?.06:st==='work'?Math.sin(t*.6+s.phase)*.035:0;
      const blink=st==='rest'?.1:st==='work'||st==='talk'?Math.sin(t*(.8+s.phase*.08))>.99?.15:1:1;
      s.eyes.forEach(eye=>eye.scale.y=.45*blink);
      s.brows.forEach((b,i)=>b.rotation.z=st==='stop'?(i?-.2:.2):0);
      s.mouth.scale.y=st==='talk'?1.3+Math.abs(Math.sin(t*9)):1;s.mouth.rotation.z=st==='happy'?.06:0;
      s.dome.visible=id==='rk'&&st==='stop';if(s.emergency)s.emergency.material.emissive.set(st==='stop'?0xD23B3B:0x000000);
      s.zz.visible=st==='rest';
      if(s.bookCover){
        if(id==='rv')s.bookCover.rotation.x=st==='work'?-.25-Math.abs(Math.sin(t*1.7))*.65:st==='talk'?0:-.12;
        else if(id==='pm')s.bookCover.rotation.x=st==='work'?-.3-Math.abs(Math.sin(t*1.35))*.6:0;
        else if(id==='a1')s.bookCover.rotation.x=0;
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
  function animateRelay(now){if(disposed)return;render(now);if(relayMotion&&!document.hidden)relayRaf=requestAnimationFrame(animateRelay);else relayRaf=0;}
  const intersection=new IntersectionObserver(entries=>{visible=entries[0].isIntersecting;sync();});intersection.observe(host);
  const visibility=()=>sync();document.addEventListener('visibilitychange',visibility);
  const reductionChanged=()=>{reduce=reduceQuery.matches;sync();};reduceQuery.addEventListener('change',reductionChanged);
  const ro=new ResizeObserver(resize);ro.observe(host);resize();sync();
  renderer.domElement.addEventListener('webglcontextlost',event=>{event.preventDefault();cancelAnimationFrame(raf);onUnavailable('WEBGL_CONTEXT_LOST');});
  return {
    focus(id){if(id!=='all'&&!stations.has(id))return;focus=id;resize();},
    relay(from,to){
      const a=ROLES.findIndex(r=>r.id===from),b=ROLES.findIndex(r=>r.id===to);
      if(a<0||b<0||a===b)return;
      relayDot.visible=true;relayMotion={from:a,to:b,start:performance.now()};
      stageContext.currentRole=to;cancelAnimationFrame(relayRaf);
      if(reduce)render(performance.now()+720);else relayRaf=requestAnimationFrame(animateRelay);
    },
    setContext({market,currentRole}={}){
      let changed=false;
      if(typeof market==='string'&&market.trim()&&stageContext.market!==market.trim()){stageContext.market=market.trim().slice(0,24);changed=true;}
      if(stations.has(currentRole)&&stageContext.currentRole!==currentRole){
        stageContext.currentRole=currentRole;changed=true;
        if(!relayMotion){relayDot.visible=true;relayDot.position.copy(relayPoints[ROLES.findIndex(r=>r.id===currentRole)]);relayDot.position.y=.1;}
      }
      if(changed)render(performance.now());
    },
    setRole(id,state){if(!stations.has(id)||!STATES.includes(state))return;clearTimeout(happyTimers.get(id));happyTimers.delete(id);const s=stations.get(id);s.state=state;s.since=performance.now();if(state==='happy'&&(!preview||!paused))armHappyTimeout(id);render(performance.now());},
    setAll(state){for(const r of ROLES)this.setRole(r.id,state);},
    pause(value){paused=Boolean(value);if(!paused)for(const [id,s]of stations)if(s.state==='happy')armHappyTimeout(id);sync();},
    setView(mode){view=mode;scene.overrideMaterial=mode==='silhouette'?shaderSilhouette:null;environment.visible=mode!=='silhouette';theme();render(performance.now());},
    dispose(){disposed=true;cancelAnimationFrame(raf);cancelAnimationFrame(relayRaf);for(const timer of happyTimers.values())clearTimeout(timer);happyTimers.clear();ro.disconnect();intersection.disconnect();observer.disconnect();reduceQuery.removeEventListener('change',reductionChanged);document.removeEventListener('visibilitychange',visibility);geometries.forEach(g=>g.dispose());materials.forEach(m=>m.dispose());textures.forEach(t=>t.dispose());renderer.dispose();renderer.domElement.remove();}
  };
}
