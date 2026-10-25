import React from 'react';
import {Composition, registerRoot} from 'remotion';
import {Video,timeline} from './Video';
const Root:React.FC=()=>React.createElement(Composition,{
  id:timeline.id, component:Video, durationInFrames:timeline.durationInFrames,
  fps:timeline.fps,width:1920,height:1080,
});
registerRoot(Root);
