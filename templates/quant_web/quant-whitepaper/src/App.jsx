import React, { useState, useRef, useEffect } from 'react';
import { motion, useScroll, AnimatePresence } from 'framer-motion';
import { ChevronsDown, Check, Fingerprint, AlertCircle, ExternalLink } from 'lucide-react';

const INK='#0A0E14', INK2='#0F151E', GREEN='#34E0A1', RED='#EF5B5B', GOLD='#E7C884',
  TXT='#E9ECF2', BODY='#BCC2CE', DIM='#8A93A3', HAIR='rgba(255,255,255,0.08)';
const SANS="'PingFang SC','Source Han Sans SC','Noto Sans SC',system-ui,-apple-system,sans-serif";
const SERIF="'Noto Serif SC','Songti SC','STSong','Source Han Serif SC',serif";
const MONO="'SF Mono','Roboto Mono',ui-monospace,Menlo,monospace";
const EASE=[0.22,1,0.36,1];

const smooth=(p)=>{if(p.length<2)return'';let d=`M ${p[0][0]} ${p[0][1]}`;for(let i=0;i<p.length-1;i++){const a=p[i-1]||p[i],b=p[i],c=p[i+1],e=p[i+2]||c;d+=` C ${b[0]+(c[0]-a[0])/6} ${b[1]+(c[1]-a[1])/6} ${c[0]-(e[0]-b[0])/6} ${c[1]-(e[1]-b[1])/6} ${c[0]} ${c[1]}`;}return d;};
const poly=(p)=>p.map((q,i)=>(i?'L':'M')+q[0]+' '+q[1]).join(' ');
const area=(p,base)=>`M ${p[0][0]} ${base} `+p.map(q=>`L ${q[0]} ${q[1]}`).join(' ')+` L ${p[p.length-1][0]} ${base} Z`;

const Reveal=({children,delay=0,y=24,className=''})=>(
  <motion.div className={className} initial={{opacity:0,y}} whileInView={{opacity:1,y:0}}
    viewport={{once:true,amount:0.35}} transition={{duration:0.8,delay,ease:EASE}}>{children}</motion.div>
);

const SectionLabel=({idx,zh,en})=>(
  <Reveal>
    <div className="flex items-center gap-3">
      <span style={{fontFamily:MONO,color:GREEN}} className="text-sm font-medium">{idx}</span>
      <span className="h-px w-10" style={{background:`linear-gradient(90deg,${GREEN},transparent)`}}/>
      <span style={{fontFamily:MONO,color:DIM}} className="text-xs tracking-widest uppercase">{zh} · {en}</span>
    </div>
  </Reveal>
);

const GoldenQuote=({children})=>(
  <Reveal delay={0.05}>
    <div className="mt-6 flex gap-3">
      <span className="w-0.5 rounded-full" style={{background:`linear-gradient(${GOLD},transparent)`}}/>
      <div>
        <div style={{fontFamily:MONO,color:GOLD}} className="mb-1 text-xs tracking-widest opacity-70">金句 · MAXIM</div>
        <p style={{fontFamily:SERIF,color:GOLD,textShadow:'0 0 24px rgba(231,200,132,0.25)'}}
          className="text-2xl font-medium leading-snug tracking-wide">{children}</p>
      </div>
    </div>
  </Reveal>
);

const StrategyTag=({children})=>(
  <Reveal delay={0.1}>
    <div className="mt-6 inline-flex items-center gap-2.5 rounded-full border px-4 py-2.5"
      style={{borderColor:'rgba(52,224,161,0.28)',background:'rgba(52,224,161,0.06)'}}>
      <span style={{fontFamily:MONO,color:GREEN}} className="text-xs font-semibold tracking-wider">本策略</span>
      <span className="h-3 w-px" style={{background:'rgba(52,224,161,0.4)'}}/>
      <span style={{color:TXT}} className="text-sm font-medium">{children}</span>
    </div>
  </Reveal>
);

const ChartFrame=({label,caption,children})=>(
  <Reveal delay={0.12}>
    <figure className="mt-8 overflow-hidden rounded-2xl border p-4"
      style={{borderColor:HAIR,background:'linear-gradient(180deg,#0F151E,#0B1118)'}}>
      {label&&<figcaption style={{fontFamily:MONO,color:DIM}} className="mb-2 text-xs tracking-wider">{label}</figcaption>}
      {children}
      {caption&&<div style={{color:DIM}} className="mt-3 text-xs leading-relaxed">{caption}</div>}
    </figure>
  </Reveal>
);

const MartingaleChart=()=>{
  const g=[[10,165],[40,150],[60,160],[90,134],[115,148],[145,118],[170,132],[200,96],[225,110],[255,72],[280,86],[312,46]];
  const r=[[10,160],[60,150],[110,140],[160,130],[210,120],[245,113]];
  return(
    <svg viewBox="0 0 320 200" className="w-full h-56">
      <line x1="6" y1="175" x2="314" y2="175" stroke={HAIR}/>
      <text x="8" y="190" style={{fontFamily:MONO}} fontSize="8" fill={DIM}>ZERO · 归零线</text>
      <motion.path d={poly(r)+' L 245 184 L 280 184'} fill="none" stroke={RED} strokeWidth="2.2" strokeLinecap="round"
        initial={{pathLength:0}} whileInView={{pathLength:1}} viewport={{once:true,amount:0.5}} transition={{duration:1.8,ease:'easeInOut'}}/>
      <motion.circle cx="263" cy="184" r="4" fill={RED} initial={{opacity:0,scale:0}} whileInView={{opacity:1,scale:1}} viewport={{once:true,amount:0.5}} transition={{delay:1.8}}/>
      <motion.circle cx="263" cy="184" r="4" fill="none" stroke={RED} strokeWidth="1.5"
        initial={{opacity:0}} whileInView={{opacity:[0.8,0],scale:[1,3]}} viewport={{once:true,amount:0.5}} transition={{delay:1.9,duration:1.4,repeat:Infinity}}/>
      <motion.text x="280" y="178" textAnchor="end" style={{fontFamily:MONO}} fontSize="9" fill={RED}
        initial={{opacity:0}} whileInView={{opacity:1}} viewport={{once:true,amount:0.5}} transition={{delay:2}}>−100% 归零</motion.text>
      <motion.text x="58" y="138" style={{fontFamily:MONO}} fontSize="9" fill={RED}
        initial={{opacity:0}} whileInView={{opacity:1}} viewport={{once:true,amount:0.5}} transition={{delay:0.8}}>马丁策略</motion.text>
      <motion.path d={poly(g)} fill="none" stroke={GREEN} strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round"
        initial={{pathLength:0}} whileInView={{pathLength:1}} viewport={{once:true,amount:0.5}} transition={{duration:1.8,ease:'easeInOut',delay:0.4}}/>
      <motion.text x="300" y="38" textAnchor="end" style={{fontFamily:MONO}} fontSize="9" fill={GREEN}
        initial={{opacity:0}} whileInView={{opacity:1}} viewport={{once:true,amount:0.5}} transition={{delay:2.2}}>本策略 · 阶梯向上</motion.text>
    </svg>
  );
};

const RecoveryChart = () => {
  const cx = 95;
  const rows = [
    { label: '日常波动', down: 10, up: 11.1, downW: 10, upW: 11, color: DIM },
    { label: '本策略极限', down: 20.5, up: 25.8, downW: 20.5, upW: 25.8, color: GREEN, focus: true },
    { label: '腰斩警戒线', down: 50, up: 100, downW: 50, upW: 100, color: GOLD },
    { label: '万劫不复', down: 90, up: 900, downW: 90, upW: 170, color: RED, isMax: true }
  ];

  return (
    <svg viewBox="0 0 320 230" className="w-full h-64">
      <defs>
        <linearGradient id="fadeRed" x1="0" y1="0" x2="1" y2="0">
          <stop offset="0%" stopColor={RED} stopOpacity="1"/>
          <stop offset="60%" stopColor={RED} stopOpacity="0.8"/>
          <stop offset="100%" stopColor={INK} stopOpacity="0"/>
        </linearGradient>
        <linearGradient id="focusGreen" x1="0" y1="0" x2="1" y2="0">
          <stop offset="0%" stopColor={GREEN} stopOpacity="0.15"/>
          <stop offset="100%" stopColor={GREEN} stopOpacity="0"/>
        </linearGradient>
      </defs>

      <text x={cx - 10} y="20" style={{fontFamily:MONO}} fontSize="9" fill={DIM} textAnchor="end">跌幅 ↓</text>
      <line x1={cx} y1="12" x2={cx} y2="24" stroke={HAIR} />
      <text x={cx + 10} y="20" style={{fontFamily:MONO}} fontSize="9" fill={DIM} textAnchor="start">回本需涨幅 ↑</text>

      <motion.line x1={cx} y1="35" x2={cx} y2="225" stroke={HAIR} strokeDasharray="2 3"
        initial={{pathLength:0}} whileInView={{pathLength:1}} transition={{duration:1}} />

      {rows.map((r, i) => {
        const y = 55 + i * 48;
        return (
          <g key={i}>
            {r.focus && (
              <motion.rect x="0" y={y-22} width="320" height="42" fill="url(#focusGreen)"
                initial={{opacity:0}} whileInView={{opacity:1}} transition={{delay:0.5}} />
            )}
            <text x={cx + 10} y={y - 12} style={{fontFamily:SANS}} fontSize="10" fill={r.focus ? GREEN : TXT} opacity={r.focus ? 1 : 0.6}>
              {r.label}
            </text>
            <motion.rect
              x={cx - r.downW} y={y - 4} width={r.downW} height="8" rx="2"
              fill={r.focus ? 'rgba(52,224,161,0.3)' : 'rgba(239,91,91,0.4)'}
              style={{ transformOrigin: 'right' }}
              initial={{ scaleX: 0 }} whileInView={{ scaleX: 1 }}
              viewport={{ once: true, amount: 0.5 }}
              transition={{ duration: 0.8, delay: i * 0.15 + 0.2, ease: 'easeOut' }}
            />
            <motion.text x={cx - r.downW - 8} y={y + 4} style={{fontFamily:MONO}} fontSize="10" fill={r.focus ? GREEN : RED} textAnchor="end"
              initial={{ opacity: 0, x: 5 }} whileInView={{ opacity: 1, x: 0 }} transition={{ delay: i * 0.15 + 0.6 }}>
              −{r.down}%
            </motion.text>
            <motion.rect
              x={cx} y={y - 4} width={r.upW} height="8" rx="2"
              fill={r.focus ? GREEN : (r.isMax ? 'url(#fadeRed)' : TXT)}
              style={{ transformOrigin: 'left' }}
              initial={{ scaleX: 0 }} whileInView={{ scaleX: 1 }}
              viewport={{ once: true, amount: 0.5 }}
              transition={{ duration: 1.2, delay: i * 0.15 + 0.3, type: 'spring', bounce: 0.25 }}
            />
            <motion.text x={cx + r.upW + (r.isMax ? 0 : 8)} y={y + 4} style={{fontFamily:MONO}} fontSize="10" fill={r.focus ? GREEN : (r.isMax ? RED : TXT)} textAnchor="start"
              initial={{ opacity: 0, x: -5 }} whileInView={{ opacity: 1, x: 0 }} transition={{ delay: i * 0.15 + 0.8 }}>
              +{r.up}% {r.isMax && <tspan fill={RED} fontSize="12" dy="1">∞</tspan>}
            </motion.text>
          </g>
        )
      })}
    </svg>
  );
};

const BalanceScale=()=>{
  const loss=[[50,108],[62,108],[74,108],[55,98],[67,98],[61,89],[44,116],[80,116]];
  const gain=[[236,104],[262,104],[249,82]];
  return(
    <svg viewBox="0 0 320 200" className="w-full h-56">
      <line x1="40" y1="180" x2="280" y2="180" stroke={HAIR}/>
      <path d="M160 118 L150 180 L170 180 Z" fill="rgba(255,255,255,0.05)" stroke={HAIR}/>
      <motion.g style={{transformBox:'view-box',transformOrigin:'160px 118px'}}
        initial={{rotate:0}} whileInView={{rotate:7}} viewport={{once:true,amount:0.5}} transition={{delay:1,duration:1.2,ease:EASE}}>
        <rect x="40" y="116" width="240" height="5" rx="2.5" fill={TXT} opacity="0.85"/>
        <circle cx="160" cy="118" r="6" fill={INK} stroke={TXT}/>
        {loss.map((p,i)=>(<motion.circle key={'l'+i} cx={p[0]} cy={p[1]} r="6" fill={RED}
          initial={{opacity:0,cy:p[1]-30}} whileInView={{opacity:0.9,cy:p[1]}} viewport={{once:true,amount:0.5}}
          transition={{delay:0.1+i*0.06,type:'spring',stiffness:200,damping:14}}/>))}
        {gain.map((p,i)=>(<motion.circle key={'g'+i} cx={p[0]} cy={p[1]} r="13" fill={GREEN}
          initial={{opacity:0,cy:p[1]-30}} whileInView={{opacity:0.95,cy:p[1]}} viewport={{once:true,amount:0.5}}
          transition={{delay:0.3+i*0.12,type:'spring',stiffness:200,damping:14}}/>))}
      </motion.g>
      <text x="62" y="160" textAnchor="middle" style={{fontFamily:MONO}} fontSize="9" fill={RED}>亏损 · 多而小</text>
      <text x="250" y="160" textAnchor="middle" style={{fontFamily:MONO}} fontSize="9" fill={GREEN}>盈利 · 少而大</text>
    </svg>
  );
};

const BearChart=()=>{
  const bench=[[10,45],[55,55],[105,72],[155,95],[205,114],[255,126],[290,131]];
  const strat=[[10,45],[55,42],[105,46],[155,40],[205,44],[255,37],[290,34]];
  return(
    <svg viewBox="0 0 300 150" className="w-full h-40">
      <path d={poly([...strat,...[...bench].reverse()])+' Z'} fill="rgba(52,224,161,0.12)"/>
      <line x1="6" y1="45" x2="294" y2="45" stroke={HAIR} strokeDasharray="3 3"/>
      <motion.path d={smooth(bench)} fill="none" stroke={RED} strokeWidth="2" initial={{pathLength:0}} whileInView={{pathLength:1}} viewport={{once:true,amount:0.5}} transition={{duration:1.4,ease:'easeInOut'}}/>
      <motion.path d={smooth(strat)} fill="none" stroke={GREEN} strokeWidth="2.2" initial={{pathLength:0}} whileInView={{pathLength:1}} viewport={{once:true,amount:0.5}} transition={{duration:1.4,ease:'easeInOut',delay:0.3}}/>
      <text x="8" y="40" style={{fontFamily:MONO}} fontSize="8" fill={DIM}>0</text>
    </svg>
  );
};

const BullChart=()=>{
  const strat=[[10,128],[50,118],[90,104],[130,86],[170,64],[210,44],[250,28],[290,16]];
  const bench=[[10,128],[55,118],[105,108],[155,98],[205,90],[255,84],[290,80]];
  return(
    <svg viewBox="0 0 300 150" className="w-full h-40">
      <defs><linearGradient id="bullg" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor={GREEN} stopOpacity="0.28"/><stop offset="100%" stopColor={GREEN} stopOpacity="0"/></linearGradient></defs>
      <motion.path d={area(strat,140)} fill="url(#bullg)" initial={{opacity:0}} whileInView={{opacity:1}} viewport={{once:true,amount:0.5}} transition={{duration:1,delay:0.6}}/>
      <motion.path d={smooth(bench)} fill="none" stroke={DIM} strokeWidth="1.6" strokeDasharray="4 4" initial={{pathLength:0}} whileInView={{pathLength:1}} viewport={{once:true,amount:0.5}} transition={{duration:1.4}}/>
      <motion.path d={smooth(strat)} fill="none" stroke={GREEN} strokeWidth="2.4" strokeLinecap="round" initial={{pathLength:0}} whileInView={{pathLength:1}} viewport={{once:true,amount:0.5}} transition={{duration:1.6,ease:'easeInOut',delay:0.3}}/>
    </svg>
  );
};

const SystematicChart=()=>{
  const line=[[10,120],[45,112],[80,101],[115,92],[150,79],[185,68],[220,53],[255,40],[290,28]];
  return(
    <svg viewBox="0 0 300 150" className="w-full h-44">
      <defs><linearGradient id="sysg" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor={GREEN} stopOpacity="0.22"/><stop offset="100%" stopColor={GREEN} stopOpacity="0"/></linearGradient></defs>
      {[30,60,90,120].map(y=><line key={y} x1="6" y1={y} x2="294" y2={y} stroke={HAIR}/>)}
      {[50,100,150,200,250].map(x=><line key={x} x1={x} y1="10" x2={x} y2="135" stroke={HAIR}/>)}
      <motion.rect x="0" y="10" width="2" height="125" fill={GREEN} opacity="0.25" initial={{x:8}} animate={{x:[8,288]}} transition={{duration:4,repeat:Infinity,ease:'linear'}}/>
      <motion.path d={area(line,135)} fill="url(#sysg)" initial={{opacity:0}} whileInView={{opacity:1}} viewport={{once:true,amount:0.5}} transition={{duration:1,delay:0.8}}/>
      <motion.path d={smooth(line)} fill="none" stroke={GREEN} strokeWidth="2.4" strokeLinecap="round" initial={{pathLength:0}} whileInView={{pathLength:1}} viewport={{once:true,amount:0.5}} transition={{duration:1.8,ease:'easeInOut'}}/>
      {line.map((p,i)=>(<motion.circle key={i} cx={p[0]} cy={p[1]} r="2.6" fill={INK} stroke={GREEN} strokeWidth="1.5"
        initial={{opacity:0,scale:0}} whileInView={{opacity:1,scale:1}} viewport={{once:true,amount:0.5}} transition={{delay:0.5+i*0.13}}/>))}
    </svg>
  );
};

const RuleStatus=()=>{
  const rules=['RULE_01  趋势确认','RULE_02  仓位控制','RULE_03  止损纪律','RULE_04  系统执行'];
  return(
    <div className="mt-4 flex flex-col gap-2">
      {rules.map((r,i)=>(
        <Reveal key={i} delay={0.5+i*0.15}>
          <div className="flex items-center justify-between rounded-lg px-3 py-2" style={{background:'rgba(255,255,255,0.03)'}}>
            <span style={{fontFamily:MONO,color:DIM}} className="text-xs tracking-wider">{r}</span>
            <span style={{fontFamily:MONO,color:GREEN}} className="flex items-center gap-1 text-xs"><Check size={12} strokeWidth={3}/> OK</span>
          </div>
        </Reveal>
      ))}
      <Reveal delay={1.2}>
        <div className="mt-1 flex items-center gap-2" style={{color:GREEN,fontFamily:MONO}}>
          <motion.span animate={{opacity:[1,0.3,1]}} transition={{repeat:Infinity,duration:1.6}} className="inline-block h-1.5 w-1.5 rounded-full" style={{background:GREEN}}/>
          <span className="text-xs tracking-widest">SYSTEM · ACTIVE · 安静运行</span>
        </div>
      </Reveal>
    </div>
  );
};

const ScrollBar=()=>{
  const {scrollYProgress}=useScroll();
  return <motion.div className="fixed left-0 top-0 z-50 h-0.5 w-full" style={{scaleX:scrollYProgress,transformOrigin:'0%',background:GREEN}}/>;
};

const Hero=()=>(
  <section className="relative flex min-h-screen flex-col justify-center py-24">
    <svg viewBox="0 0 320 200" preserveAspectRatio="xMidYMid slice" className="pointer-events-none absolute inset-x-0 bottom-10 w-full opacity-10">
      <path d={poly([[0,180],[60,170],[60,150],[120,140],[120,120],[190,110],[190,86],[255,74],[255,50],[320,36]])} fill="none" stroke={GREEN} strokeWidth="1.5"/>
    </svg>
    <motion.div initial={{opacity:0,y:16}} animate={{opacity:1,y:0}} transition={{duration:0.8,ease:EASE}}
      style={{fontFamily:MONO,color:DIM}} className="mb-10 flex items-center gap-2 text-xs tracking-widest">
      <span className="inline-block h-1.5 w-1.5 rounded-full" style={{background:GREEN}}/> ALPHA RESEARCH · 量化策略白皮书
    </motion.div>
    <motion.h1 initial={{opacity:0,y:20}} animate={{opacity:1,y:0}} transition={{duration:0.9,delay:0.15,ease:EASE}}
      style={{fontFamily:SERIF,color:TXT}} className="text-4xl font-semibold leading-tight tracking-wide">
      在市场中，<br/>真正能<span style={{color:GREEN}}>赚钱</span>的<br/>策略是什么？
    </motion.h1>
    <motion.p initial={{opacity:0,y:20}} animate={{opacity:1,y:0}} transition={{duration:0.9,delay:0.4,ease:EASE}}
      style={{color:DIM}} className="mt-7 text-base leading-relaxed">
      穿越周期的，从来不是预测的<span style={{color:TXT}}>胜率</span>，而是盈亏的<span style={{color:TXT}}>非对称结构</span>。
    </motion.p>
    <motion.div initial={{opacity:0,y:20}} animate={{opacity:1,y:0}} transition={{duration:0.9,delay:0.65,ease:EASE}} className="mt-10">
      <p style={{fontFamily:SERIF,color:GOLD,textShadow:'0 0 30px rgba(231,200,132,0.3)'}} className="text-3xl font-semibold tracking-wide">不求常胜，但求大胜。</p>
      <p style={{fontFamily:MONO,color:DIM}} className="mt-4 text-sm tracking-widest uppercase">Structure over Prediction</p>
    </motion.div>
    <motion.div initial={{opacity:0}} animate={{opacity:1}} transition={{delay:1.2,duration:1}}
      className="absolute inset-x-0 bottom-8 flex flex-col items-center gap-1" style={{color:DIM}}>
      <motion.div animate={{y:[0,6,0]}} transition={{repeat:Infinity,duration:1.8}}><ChevronsDown size={18}/></motion.div>
      <span style={{fontFamily:MONO}} className="text-xs tracking-widest">SCROLL</span>
    </motion.div>
  </section>
);

const Principle=({idx,zh,en,body,quote,strategy,caption,chartLabel,children})=>(
  <section className="flex min-h-screen flex-col justify-center py-24">
    <SectionLabel idx={idx} zh={zh} en={en}/>
    <Reveal delay={0.05}><p style={{color:BODY}} className="mt-7 text-xl font-light leading-relaxed">{body}</p></Reveal>
    <GoldenQuote>{quote}</GoldenQuote>
    <StrategyTag>{strategy}</StrategyTag>
    <ChartFrame label={chartLabel} caption={caption}>{children}</ChartFrame>
  </section>
);

const WeChatIcon = ({ size = 24, className = "" }) => (
  <svg viewBox="0 0 1024 1024" width={size} height={size} className={className} fill="currentColor">
    <path d="M682.667 768c-23.467 0-46.934-4.267-66.134-10.667L541.867 800c-12.8 6.4-27.734 0-32-10.667-2.134-8.533-2.134-14.933 0-21.333l19.2-57.6c-49.067-34.133-78.934-83.2-78.934-138.667 0-100.266 98.134-181.333 219.734-181.333 119.466 0 219.733 81.067 219.733 181.333s-98.133 181.333-219.733 181.334z m130.133-270.933c10.667 0 19.2-8.534 19.2-19.2s-8.533-19.2-19.2-19.2-19.2 8.533-19.2 19.2 8.533 19.2 19.2 19.2z m-162.133-38.4c-10.667 0-19.2 8.533-19.2 19.2s8.533 19.2 19.2 19.2 19.2-8.533 19.2-19.2-8.533-19.2-19.2-19.2zM401.067 661.333c-36.267 0-70.4-10.667-100.267-25.6l-98.133 53.334c-17.067 8.533-36.267 0-42.667-14.934-2.133-10.666-2.133-19.2 0-29.866l23.467-78.934c-61.867-51.2-100.267-117.333-100.267-194.133 0-145.067 140.8-262.4 313.6-262.4 174.933 0 315.733 117.333 315.733 262.4 0 27.733-4.267 55.467-12.8 81.067-10.667-2.133-21.333-2.133-32-2.133-142.933 0-258.133 100.267-258.133 226.133 0 59.733 27.733 115.2 76.8 153.6-27.733 21.333-57.6 32-85.333 32z m-123.734-352c-14.933 0-27.733 12.8-27.733 27.734s12.8 27.733 27.733 27.733 27.733-12.8 27.733-27.733-12.8-27.734-27.733-27.734z m219.734 0c-14.934 0-27.734 12.8-27.734 27.734s12.8 27.733 27.734 27.733 27.733-12.8 27.733-27.733-12.8-27.734-27.733-27.734z"/>
  </svg>
);

const Finale=()=>{
  const HOLD=2400;
  const feats=['不预测市场，只跟随趋势','先求不死，再求大胜','亏有底线，赢无上限','熊市不亏，牛市起飞','规则驱动，透明可验'];
  const TH=[0.12,0.30,0.48,0.66,0.84];
  const [progress,setProgress]=useState(0),
        [holding,setHolding]=useState(false),
        [done,setDone]=useState(false),
        [val,setVal]=useState(0),
        [showModal,setShowModal]=useState(false);

  const pRef=useRef(0),mode=useRef('idle'),raf=useRef(0),last=useRef(0);
  const progressSpanRef = useRef(null);
  const buttonRef = useRef(null);

  const loop = now => {
    const dt = now - last.current; last.current = now;
    if (mode.current === 'fill') {
      let p = pRef.current + dt / HOLD;

      if (p >= 1) {
        pRef.current = 1;
        setProgress(1);
        if (progressSpanRef.current) progressSpanRef.current.style.width = '100%';
        if (buttonRef.current) buttonRef.current.style.boxShadow = `0 0 35px rgba(52,224,161,0.35)`;
        mode.current = 'idle';
        setHolding(false);
        setDone(true);
        return;
      }

      const oldRevealed = TH.filter(t => pRef.current >= t).length;
      const newRevealed = TH.filter(t => p >= t).length;
      if (newRevealed !== oldRevealed) setProgress(p);

      pRef.current = p;
      if (progressSpanRef.current) progressSpanRef.current.style.width = `${p * 100}%`;
      if (buttonRef.current) buttonRef.current.style.boxShadow = `0 0 ${15 + p * 20}px rgba(52,224,161,${0.15 + p * 0.2})`;

      raf.current = requestAnimationFrame(loop);

    } else if (mode.current === 'decay') {
      let p = pRef.current - dt / 500;

      if (p <= 0) {
        pRef.current = 0;
        setProgress(0);
        if (progressSpanRef.current) progressSpanRef.current.style.width = '0%';
        if (buttonRef.current) buttonRef.current.style.boxShadow = `0 0 15px rgba(52,224,161,0.15)`;
        mode.current = 'idle';
        return;
      }

      const oldRevealed = TH.filter(t => pRef.current >= t).length;
      const newRevealed = TH.filter(t => p >= t).length;
      if (newRevealed !== oldRevealed) setProgress(p);

      pRef.current = p;
      if (progressSpanRef.current) progressSpanRef.current.style.width = `${p * 100}%`;
      if (buttonRef.current) buttonRef.current.style.boxShadow = `0 0 ${15 + p * 20}px rgba(52,224,161,${0.15 + p * 0.2})`;

      raf.current = requestAnimationFrame(loop);
    }
  };

  const start=(e)=>{
    if(done)return;
    if (e && e.pointerId !== undefined && e.target.setPointerCapture) {
      try { e.target.setPointerCapture(e.pointerId); } catch(err){}
    }
    mode.current='fill';
    last.current=performance.now();
    cancelAnimationFrame(raf.current);
    raf.current=requestAnimationFrame(loop);
    setHolding(true);
  };

  const end=(e)=>{
    if (e && e.pointerId !== undefined && e.target.releasePointerCapture) {
      try { e.target.releasePointerCapture(e.pointerId); } catch(err){}
    }
    if(done||mode.current!=='fill')return;
    setHolding(false);
    mode.current='decay';
    last.current=performance.now();
    cancelAnimationFrame(raf.current);
    raf.current=requestAnimationFrame(loop);
  };

  const handleApply = () => {
    try { navigator.clipboard.writeText('yys190704'); } catch (err) {}
    setShowModal(true);
  };

  useEffect(()=>()=>cancelAnimationFrame(raf.current),[]);
  useEffect(()=>{if(!done)return;let r;const s=performance.now();const t=now=>{const k=Math.min(1,(now-s)/3000);setVal(1962.9*(1-Math.pow(1-k,3)));if(k<1)r=requestAnimationFrame(t);};r=requestAnimationFrame(t);return()=>cancelAnimationFrame(r);},[done]);
  const reset=()=>{setDone(false);pRef.current=0;setProgress(0);setVal(0);mode.current='idle';};

  const revealed=TH.filter(t=>progress>=t).length;
  const eq=[[8,152],[28,150],[48,146],[68,149],[90,140],[110,143],[132,132],[154,135],[176,120],[198,123],[220,104],[242,100],[262,78],[282,58],[300,38],[314,22]];

  return(
    <section className="relative flex min-h-screen flex-col justify-center py-24">
      {done&&<div className="pointer-events-none fixed inset-0" style={{background:'radial-gradient(circle at 50% 40%, rgba(52,224,161,0.10), transparent 60%)'}}/>}
      <AnimatePresence mode="wait">
      {!done?(
          <motion.div key="pre" exit={{opacity: 0, scale: 0.96}} transition={{duration: 0.5}}
                      className="flex flex-col items-center text-center">
              <Reveal><p style={{fontFamily: SERIF, color: TXT}} className="text-3xl font-semibold leading-snug">五条法则，<br/>一条曲线。
              </p></Reveal>
              <Reveal delay={0.1}><p style={{color: DIM}} className="mt-4 text-sm leading-relaxed">理念已尽数陈述。<br/>现在，按住下方，见证它穿越牛熊的威力。
              </p></Reveal>
              <div className="mt-8 mb-8 flex w-full flex-col gap-2.5">
                  {feats.map((f, i) => (
                      <motion.div key={i} animate={{opacity: i < revealed ? 1 : 0.18}} transition={{duration: 0.4}}
                                  className="flex items-center gap-3 rounded-xl border px-4 py-2.5"
                                  style={{
                                      borderColor: i < revealed ? 'rgba(52,224,161,0.3)' : HAIR,
                                      background: i < revealed ? 'rgba(52,224,161,0.06)' : 'transparent'
                                  }}>
                          <motion.span animate={{scale: i < revealed ? 1 : 0.6}}
                                       className="flex h-5 w-5 items-center justify-center rounded-full"
                                       style={{
                                           background: i < revealed ? GREEN : 'transparent',
                                           border: i < revealed ? 'none' : `1px solid ${HAIR}`
                                       }}>
                              {i < revealed && <Check size={13} color={INK} strokeWidth={3}/>}
                          </motion.span>
                          <span style={{color: i < revealed ? TXT : DIM}} className="text-sm font-medium">{f}</span>
                      </motion.div>
                  ))}
              </div>
              <button onPointerDown={start} onPointerUp={end} onPointerLeave={end} onPointerCancel={end}
                      onContextMenu={e => e.preventDefault()}
                      onTouchStart={e => e.preventDefault()}
                      ref={buttonRef}
                      style={{
                          touchAction: 'none',
                          userSelect: 'none',
                          WebkitUserSelect: 'none',
                          WebkitTouchCallout: 'none',
                          WebkitTapHighlightColor: 'transparent',
                          borderColor: GREEN,
                          transform: 'translateZ(0)',
                          boxShadow: `0 0 ${15 + pRef.current * 20}px rgba(52,224,161,${0.15 + pRef.current * 0.2})`
                      }}
                      className="relative w-full overflow-hidden rounded-full border-2 px-6 py-4">

                  <span ref={progressSpanRef} className="absolute inset-y-0 left-0" style={{
                      width: `${pRef.current * 100}%`,
                      background: 'rgba(52,224,161,0.22)'
                  }}/>

                  <span className="relative flex items-center justify-center gap-2"
                        style={{color: GREEN, transform: 'translateZ(0)'}}>
              <Fingerprint size={18}/>
              <span
                  className="text-base font-semibold tracking-wide">{holding ? '持续按住…' : '按住 · 见识穿越牛熊的威力'}</span>
            </span>
              </button>
              <motion.p animate={{opacity: holding ? 0.4 : 0.7}} style={{fontFamily: MONO, color: DIM}}
                        className="mt-4 text-xs tracking-widest">
                  {holding ? 'LOADING ···' : 'PRESS & HOLD · 长按解锁'}</motion.p>
          </motion.div>
      ) : (
          <motion.div key="post" initial={{opacity: 0, scale: 0.96}} animate={{opacity: 1, scale: 1}}
                      transition={{duration: 0.6, ease: EASE}} className="w-full">
              <p style={{fontFamily: MONO, color: DIM}} className="text-xs tracking-widest uppercase">Cumulative Return
                  · 累计收益率 · 回测</p>
              <div className="mt-2 flex items-end gap-1">
                  <span style={{fontFamily: MONO, color: GOLD}} className="text-2xl font-bold">+</span>
                  <span style={{fontFamily: MONO, color: GOLD, textShadow: '0 0 40px rgba(231,200,132,0.4)'}}
                        className="text-6xl font-bold tracking-tight tabular-nums">{val.toFixed(1)}</span>
                  <span style={{fontFamily: MONO, color: GOLD}} className="mb-2 text-3xl font-bold">%</span>
              </div>
              <div className="mt-6 rounded-2xl border p-4"
                   style={{borderColor: HAIR, background: 'linear-gradient(180deg,#0F151E,#0B1118)'}}>
                  <svg viewBox="0 0 320 170" className="w-full h-44">
                      <defs>
                          <linearGradient id="eqg" x1="0" y1="0" x2="0" y2="1">
                              <stop offset="0%" stopColor={GREEN} stopOpacity="0.3"/>
                              <stop offset="100%" stopColor={GREEN} stopOpacity="0"/>
                          </linearGradient>
                      </defs>
                      <motion.path d={area(eq, 160)} fill="url(#eqg)" initial={{opacity: 0}} animate={{opacity: 1}}
                                   transition={{duration: 2, delay: 0.4}}/>
                      <motion.path d={smooth(eq)} fill="none" stroke={GREEN} strokeWidth="2.6" strokeLinecap="round" initial={{pathLength:0}} animate={{pathLength:1}} transition={{duration:3,ease:'easeOut'}}/>
              <line x1="6" y1="160" x2="314" y2="160" stroke={HAIR}/>
            </svg>
          </div>
          <div className="mt-5 grid grid-cols-2 gap-3">
            <div className="rounded-xl border p-4" style={{borderColor:HAIR}}>
              <p style={{fontFamily:MONO,color:DIM}} className="text-xs tracking-wider">全程最大回撤</p>
              <p style={{fontFamily:MONO,color:GREEN}} className="mt-1 text-2xl font-bold tabular-nums">−20.5%</p>
            </div>
            <div className="rounded-xl border p-4" style={{borderColor:HAIR}}>
              <p style={{fontFamily:MONO,color:DIM}} className="text-xs tracking-wider">收益 / 回撤</p>
              <p style={{fontFamily:MONO,color:GREEN}} className="mt-1 text-2xl font-bold tabular-nums">95.8×</p>
            </div>
          </div>

          <div className="mt-7 text-center">
            <p style={{fontFamily:SERIF,color:GOLD}} className="text-xl font-semibold">不求常胜，但求大胜。</p>
            <p style={{fontFamily:MONO,color:DIM}} className="mt-2 text-xs tracking-widest uppercase">Structure over Prediction</p>
          </div>

          {/* 加入延时出场且包裹 height 动画：在3秒前完全不占高度，让上方大字图表独占屏幕。3秒后平滑向下撑开 */}
          <motion.div
            initial={{opacity: 0, height: 0, overflow: 'hidden'}}
            animate={{opacity: 1, height: 'auto'}}
            transition={{delay: 3, duration: 0.8, ease: EASE}}
          >
            <div className="pt-10 pb-8">
              <div className="flex w-full flex-col gap-2.5">
                <p style={{fontFamily:MONO,color:DIM}} className="mb-2 text-center text-xs tracking-widest">贯彻五条理念 · 方能穿越牛熊</p>
                {feats.map((f, i) => (
                    <div key={i} className="flex items-center gap-3 rounded-xl border px-4 py-2.5"
                         style={{
                             borderColor: 'rgba(52,224,161,0.25)',
                             background: 'rgba(52,224,161,0.05)'
                         }}>
                        <span className="flex h-5 w-5 items-center justify-center rounded-full"
                              style={{ background: GREEN }}>
                            <Check size={13} color={INK} strokeWidth={3}/>
                        </span>
                        <span style={{color: TXT}} className="text-sm font-medium">{f}</span>
                    </div>
                ))}
              </div>

              <div className="mt-10 flex flex-col items-center gap-4">
                <button onClick={handleApply} style={{
                    background: '#07C160',
                    color: '#FFFFFF'
                  }}
                  className="flex w-full items-center justify-center gap-2 rounded-xl py-4 text-lg font-bold tracking-wide transition-transform active:scale-95"
                >
                  <WeChatIcon size={24} />
                  申请实盘白名单
                </button>
                <button onClick={reset} style={{borderColor:HAIR,color:DIM,fontFamily:MONO}} className="mt-2 rounded-full border px-4 py-2 text-xs tracking-widest">↻ 重新演示</button>
              </div>

              <p style={{color:DIM}} className="mt-8 text-center text-xs leading-relaxed opacity-60">*以上为历史回测数据，不代表未来收益，不构成投资建议。</p>
            </div>
          </motion.div>
        </motion.div>
      )}
      </AnimatePresence>

      <AnimatePresence>
        {showModal && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="fixed inset-0 z-50 flex items-center justify-center p-6 backdrop-blur-md"
            style={{ background: 'rgba(10, 14, 20, 0.85)' }}
          >
            <motion.div
              initial={{ scale: 0.95, y: 10 }}
              animate={{ scale: 1, y: 0 }}
              exit={{ scale: 0.95, y: 10 }}
              className="relative w-full max-w-sm overflow-hidden rounded-2xl border border-white/10 bg-[#0A0E14] shadow-2xl"
            >
              <div className="absolute inset-x-0 top-0 h-1 bg-gradient-to-r from-transparent via-[#07C160] to-transparent opacity-50"></div>

              <div className="flex flex-col items-center p-8 text-center">
                <div className="mb-5 flex h-14 w-14 items-center justify-center rounded-full border border-[#07C160]/20 bg-[#07C160]/10">
                  <Check className="text-[#07C160]" size={28} strokeWidth={3} />
                </div>
                <h3 className="mb-2 text-xl font-bold text-white">秘钥复制成功</h3>
                <p style={{fontFamily: MONO}} className="mb-8 text-xs text-[#8A93A3]">NODE_ADMIN_WECHAT_ID_COPIED</p>

                <div className="mb-4 w-full rounded-xl border border-white/5 bg-[#000000] py-5">
                  <p className="mb-2 text-xs text-[#8A93A3]">请在微信添加管理员:</p>
                  <p style={{fontFamily: MONO}} className="text-3xl font-bold tracking-wider text-white">yys190704</p>
                </div>

                <div className="mb-8 w-full flex items-start gap-3 rounded-xl border border-white/5 bg-[#13171F] p-4 text-left">
                  <AlertCircle size={18} className="mt-0.5 shrink-0 text-[#F5A623]" />
                  <p className="text-xs leading-relaxed text-[#BCC2CE]">
                    通关暗号：添加时请务必备注 <span className="font-bold text-[#07C160]">Alpha节点</span> ，否则系统将自动拒绝好友申请。
                  </p>
                </div>

                <button
                  onClick={() => window.location.href = 'weixin://'}
                  className="mb-4 flex w-full items-center justify-center gap-2 rounded-xl bg-[#07C160] py-3.5 text-base font-bold text-white transition-colors hover:bg-[#06ad56] active:scale-95"
                >
                  打开微信去粘贴 <ExternalLink size={18} />
                </button>

                <button
                  onClick={() => setShowModal(false)}
                  className="text-sm text-[#8A93A3] transition-colors hover:text-white"
                >
                  稍后再试，关闭窗口
                </button>
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
    </section>
  );
};

export default function App(){
  return(
    <div style={{background:INK,color:TXT,fontFamily:SANS}} className="relative min-h-screen w-full overflow-x-hidden">
      <style>{`@import url('https://fonts.googleapis.com/css2?family=Noto+Serif+SC:wght@400;500;600;700&display=swap');html{scroll-behavior:smooth;}::selection{background:rgba(52,224,161,0.3);}`}</style>
      <div className="pointer-events-none fixed inset-0" style={{background:'radial-gradient(circle at 50% 0%, rgba(52,224,161,0.06), transparent 55%)'}}/>
      <ScrollBar/>
      <div className="relative z-10 mx-auto w-full max-w-md px-6">
        <Hero/>
        <Principle idx="01" zh="不可预测" en="Unpredictable"
          body={<>市场不可预测。没有人能每次都猜对，<span style={{color:RED}}>高胜率是一种昂贵的幻觉</span>——平静的表面下，藏着致命的尾部风险。</>}
          quote="市场不可预测，规则方可长青。" strategy="不预测市场，只跟随趋势。"
          chartLabel="MARTINGALE vs 本策略"
          caption={<>红线 · 高胜率马丁：连赢之后，一次<span style={{color:RED}}>归零</span>。绿线 · 本策略：锯齿波动，<span style={{color:GREEN}}>阶梯向上</span>。</>}>
          <MartingaleChart/>
        </Principle>
        <Principle idx="02" zh="先求不死" en="Survival"
          body={<>市场每天都有机会，账户没有。没人因为赚得慢而离开，<span style={{color:RED}}>但绝大多数人，都倒在一次大亏上</span>。活下来，永远比赚快钱重要。</>}
          quote="先求不死，再求大胜。" strategy="赢到最后，比一直赢更重要。"
          chartLabel="DRAWDOWN → RECOVERY · 回撤回本"
          caption={<>亏损越深，回本越难：−50% 需 +100%，−90% 需 <span style={{color:RED}}>+900%</span>。<span style={{color:GREEN}}>本策略最大回撤仅 −20.5%</span>，回本只需 +25.8%——永不靠近深渊。</>}>
          <RecoveryChart/>
        </Principle>
        <Principle idx="03" zh="非对称" en="Asymmetry"
          body={<>决定盈亏的，是赢与亏的<span style={{color:TXT}}>倍数</span>，不是赢与亏的次数。亏损次数可以更多，只要每次都小；盈利次数可以更少，只要足够大。</>}
          quote="截断亏损，让利润奔跑。" strategy="亏有底线，赢无上限。"
          chartLabel="RISK / REWARD · 盈亏天平"
          caption={<>亏损 · 多而小 · <span style={{color:GREEN}}>有底线</span> ｜ 盈利 · 少而大 · <span style={{color:GREEN}}>无上限</span>。天平，终究倒向盈利。</>}>
          <BalanceScale/>
        </Principle>
        <Principle idx="04" zh="穿越牛熊" en="All-Weather"
          body={<>牛市里人人都是股神；熊市退潮，<span style={{color:RED}}>才知道谁在裸泳</span>。真正的复利，是熊市少亏、牛市敢赚的结果。</>}
          quote="牛市决定收益，熊市决定复利。" strategy="熊市不亏，牛市起飞。"
          chartLabel="BEAR / BULL · 穿越牛熊"
          caption={<>熊市：基准下行，策略守住<span style={{color:GREEN}}>超额收益</span>。牛市：顺势放大，收益起飞。</>}>
          <div className="flex flex-col gap-4">
            <div>
              <div className="mb-1 flex items-center justify-between">
                <span style={{fontFamily:MONO,color:RED}} className="text-xs tracking-wider">熊市 · BEAR</span>
                <span style={{fontFamily:MONO,color:DIM}} className="text-xs">基准 <span style={{color:RED}}>−38.2%</span> · 策略 <span style={{color:GREEN}}>+6.5%</span></span>
              </div>
              <BearChart/>
              <p style={{fontFamily:MONO,color:GREEN}} className="text-xs">↑ 超额收益 +44.7%</p>
            </div>
            <div className="h-px w-full" style={{background:HAIR}}/>
            <div>
              <div className="mb-1 flex items-center justify-between">
                <span style={{fontFamily:MONO,color:GREEN}} className="text-xs tracking-wider">牛市 · BULL</span>
                <span style={{fontFamily:MONO,color:DIM}} className="text-xs">基准 +120% · 策略 <span style={{color:GREEN}}>+312%</span></span>
              </div>
              <BullChart/>
            </div>
          </div>
        </Principle>
        <Principle idx="05" zh="系统驱动" en="Systematic"
          body={<>不依赖所谓的大牛喊单——<span style={{color:TXT}}>人会说谎，数据不会</span>。把每一笔交易交给规则，把情绪踢出场外，让纪律安静运行。</>}
          quote="把交易交给系统，把情绪踢出局。" strategy="规则驱动，透明可验，做时间复利的朋友。"
          chartLabel="SYSTEMATIC · 系统驱动"
          caption={<>规则恒定，纪律执行，收益随时间稳定累积——安静运行，无需人管。</>}>
          <SystematicChart/>
          <RuleStatus/>
        </Principle>
        <Finale/>
      </div>
    </div>
  );
}