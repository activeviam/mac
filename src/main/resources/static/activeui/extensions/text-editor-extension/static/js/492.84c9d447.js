(self.webpackChunktext_editor_extension=self.webpackChunktext_editor_extension||[]).push([[492],{1989:(t,r,e)=>{var n=e(1789),o=e(401),a=e(7667),i=e(1327),s=e(1866);function u(t){var r=-1,e=null==t?0:t.length;for(this.clear();++r<e;){var n=t[r];this.set(n[0],n[1])}}u.prototype.clear=n,u.prototype.delete=o,u.prototype.get=a,u.prototype.has=i,u.prototype.set=s,t.exports=u},8407:(t,r,e)=>{var n=e(7040),o=e(4125),a=e(2117),i=e(7518),s=e(4705);function u(t){var r=-1,e=null==t?0:t.length;for(this.clear();++r<e;){var n=t[r];this.set(n[0],n[1])}}u.prototype.clear=n,u.prototype.delete=o,u.prototype.get=a,u.prototype.has=i,u.prototype.set=s,t.exports=u},7071:(t,r,e)=>{var n=e(852)(e(5639),"Map");t.exports=n},3369:(t,r,e)=>{var n=e(4785),o=e(1285),a=e(6e3),i=e(9916),s=e(5265);function u(t){var r=-1,e=null==t?0:t.length;for(this.clear();++r<e;){var n=t[r];this.set(n[0],n[1])}}u.prototype.clear=n,u.prototype.delete=o,u.prototype.get=a,u.prototype.has=i,u.prototype.set=s,t.exports=u},6384:(t,r,e)=>{var n=e(8407),o=e(7465),a=e(3779),i=e(7599),s=e(4758),u=e(4309);function c(t){var r=this.__data__=new n(t);this.size=r.size}c.prototype.clear=o,c.prototype.delete=a,c.prototype.get=i,c.prototype.has=s,c.prototype.set=u,t.exports=c},2705:(t,r,e)=>{var n=e(5639).Symbol;t.exports=n},1149:(t,r,e)=>{var n=e(5639).Uint8Array;t.exports=n},6874:t=>{t.exports=function(t,r,e){switch(e.length){case 0:return t.call(r);case 1:return t.call(r,e[0]);case 2:return t.call(r,e[0],e[1]);case 3:return t.call(r,e[0],e[1],e[2])}return t.apply(r,e)}},4636:(t,r,e)=>{var n=e(2545),o=e(5694),a=e(1469),i=e(4144),s=e(5776),u=e(6719),c=Object.prototype.hasOwnProperty;t.exports=function(t,r){var e=a(t),p=!e&&o(t),f=!e&&!p&&i(t),v=!e&&!p&&!f&&u(t),l=e||p||f||v,h=l?n(t.length,String):[],_=h.length;for(var y in t)!r&&!c.call(t,y)||l&&("length"==y||f&&("offset"==y||"parent"==y)||v&&("buffer"==y||"byteLength"==y||"byteOffset"==y)||s(y,_))||h.push(y);return h}},6556:(t,r,e)=>{var n=e(9465),o=e(7813);t.exports=function(t,r,e){(void 0!==e&&!o(t[r],e)||void 0===e&&!(r in t))&&n(t,r,e)}},4865:(t,r,e)=>{var n=e(9465),o=e(7813),a=Object.prototype.hasOwnProperty;t.exports=function(t,r,e){var i=t[r];a.call(t,r)&&o(i,e)&&(void 0!==e||r in t)||n(t,r,e)}},8470:(t,r,e)=>{var n=e(7813);t.exports=function(t,r){for(var e=t.length;e--;)if(n(t[e][0],r))return e;return-1}},9465:(t,r,e)=>{var n=e(8777);t.exports=function(t,r,e){"__proto__"==r&&n?n(t,r,{configurable:!0,enumerable:!0,value:e,writable:!0}):t[r]=e}},3118:(t,r,e)=>{var n=e(3218),o=Object.create,a=function(){function t(){}return function(r){if(!n(r))return{};if(o)return o(r);t.prototype=r;var e=new t;return t.prototype=void 0,e}}();t.exports=a},8483:(t,r,e)=>{var n=e(5063)();t.exports=n},4239:(t,r,e)=>{var n=e(2705),o=e(9607),a=e(2333),i=n?n.toStringTag:void 0;t.exports=function(t){return null==t?void 0===t?"[object Undefined]":"[object Null]":i&&i in Object(t)?o(t):a(t)}},9454:(t,r,e)=>{var n=e(4239),o=e(7005);t.exports=function(t){return o(t)&&"[object Arguments]"==n(t)}},8458:(t,r,e)=>{var n=e(3560),o=e(5346),a=e(3218),i=e(346),s=/^\[object .+?Constructor\]$/,u=Function.prototype,c=Object.prototype,p=u.toString,f=c.hasOwnProperty,v=RegExp("^"+p.call(f).replace(/[\\^$.*+?()[\]{}|]/g,"\\$&").replace(/hasOwnProperty|(function).*?(?=\\\()| for .+?(?=\\\])/g,"$1.*?")+"$");t.exports=function(t){return!(!a(t)||o(t))&&(n(t)?v:s).test(i(t))}},8749:(t,r,e)=>{var n=e(4239),o=e(1780),a=e(7005),i={};i["[object Float32Array]"]=i["[object Float64Array]"]=i["[object Int8Array]"]=i["[object Int16Array]"]=i["[object Int32Array]"]=i["[object Uint8Array]"]=i["[object Uint8ClampedArray]"]=i["[object Uint16Array]"]=i["[object Uint32Array]"]=!0,i["[object Arguments]"]=i["[object Array]"]=i["[object ArrayBuffer]"]=i["[object Boolean]"]=i["[object DataView]"]=i["[object Date]"]=i["[object Error]"]=i["[object Function]"]=i["[object Map]"]=i["[object Number]"]=i["[object Object]"]=i["[object RegExp]"]=i["[object Set]"]=i["[object String]"]=i["[object WeakMap]"]=!1,t.exports=function(t){return a(t)&&o(t.length)&&!!i[n(t)]}},313:(t,r,e)=>{var n=e(3218),o=e(5726),a=e(3498),i=Object.prototype.hasOwnProperty;t.exports=function(t){if(!n(t))return a(t);var r=o(t),e=[];for(var s in t)("constructor"!=s||!r&&i.call(t,s))&&e.push(s);return e}},2980:(t,r,e)=>{var n=e(6384),o=e(6556),a=e(8483),i=e(9783),s=e(3218),u=e(1704),c=e(6390);t.exports=function t(r,e,p,f,v){r!==e&&a(e,(function(a,u){if(v||(v=new n),s(a))i(r,e,u,p,t,f,v);else{var l=f?f(c(r,u),a,u+"",r,e,v):void 0;void 0===l&&(l=a),o(r,u,l)}}),u)}},9783:(t,r,e)=>{var n=e(6556),o=e(4626),a=e(7133),i=e(278),s=e(8517),u=e(5694),c=e(1469),p=e(9246),f=e(4144),v=e(3560),l=e(3218),h=e(8630),_=e(6719),y=e(6390),x=e(9881);t.exports=function(t,r,e,b,d,j,g){var O=y(t,e),w=y(r,e),A=g.get(w);if(A)n(t,e,A);else{var m=j?j(O,w,e+"",t,r,g):void 0,z=void 0===m;if(z){var P=c(w),S=!P&&f(w),F=!P&&!S&&_(w);m=w,P||S||F?c(O)?m=O:p(O)?m=i(O):S?(z=!1,m=o(w,!0)):F?(z=!1,m=a(w,!0)):m=[]:h(w)||u(w)?(m=O,u(O)?m=x(O):l(O)&&!v(O)||(m=s(w))):z=!1}z&&(g.set(w,m),d(m,w,b,j,g),g.delete(w)),n(t,e,m)}}},5976:(t,r,e)=>{var n=e(6557),o=e(5357),a=e(61);t.exports=function(t,r){return a(o(t,r,n),t+"")}},6560:(t,r,e)=>{var n=e(5703),o=e(8777),a=e(6557),i=o?function(t,r){return o(t,"toString",{configurable:!0,enumerable:!1,value:n(r),writable:!0})}:a;t.exports=i},2545:t=>{t.exports=function(t,r){for(var e=-1,n=Array(t);++e<t;)n[e]=r(e);return n}},1717:t=>{t.exports=function(t){return function(r){return t(r)}}},4318:(t,r,e)=>{var n=e(1149);t.exports=function(t){var r=new t.constructor(t.byteLength);return new n(r).set(new n(t)),r}},4626:(t,r,e)=>{t=e.nmd(t);var n=e(5639),o=r&&!r.nodeType&&r,a=o&&t&&!t.nodeType&&t,i=a&&a.exports===o?n.Buffer:void 0,s=i?i.allocUnsafe:void 0;t.exports=function(t,r){if(r)return t.slice();var e=t.length,n=s?s(e):new t.constructor(e);return t.copy(n),n}},7133:(t,r,e)=>{var n=e(4318);t.exports=function(t,r){var e=r?n(t.buffer):t.buffer;return new t.constructor(e,t.byteOffset,t.length)}},278:t=>{t.exports=function(t,r){var e=-1,n=t.length;for(r||(r=Array(n));++e<n;)r[e]=t[e];return r}},8363:(t,r,e)=>{var n=e(4865),o=e(9465);t.exports=function(t,r,e,a){var i=!e;e||(e={});for(var s=-1,u=r.length;++s<u;){var c=r[s],p=a?a(e[c],t[c],c,e,t):void 0;void 0===p&&(p=t[c]),i?o(e,c,p):n(e,c,p)}return e}},4429:(t,r,e)=>{var n=e(5639)["__core-js_shared__"];t.exports=n},1463:(t,r,e)=>{var n=e(5976),o=e(6612);t.exports=function(t){return n((function(r,e){var n=-1,a=e.length,i=a>1?e[a-1]:void 0,s=a>2?e[2]:void 0;for(i=t.length>3&&"function"==typeof i?(a--,i):void 0,s&&o(e[0],e[1],s)&&(i=a<3?void 0:i,a=1),r=Object(r);++n<a;){var u=e[n];u&&t(r,u,n,i)}return r}))}},5063:t=>{t.exports=function(t){return function(r,e,n){for(var o=-1,a=Object(r),i=n(r),s=i.length;s--;){var u=i[t?s:++o];if(!1===e(a[u],u,a))break}return r}}},8777:(t,r,e)=>{var n=e(852),o=function(){try{var t=n(Object,"defineProperty");return t({},"",{}),t}catch(r){}}();t.exports=o},1957:(t,r,e)=>{var n="object"==typeof e.g&&e.g&&e.g.Object===Object&&e.g;t.exports=n},5050:(t,r,e)=>{var n=e(7019);t.exports=function(t,r){var e=t.__data__;return n(r)?e["string"==typeof r?"string":"hash"]:e.map}},852:(t,r,e)=>{var n=e(8458),o=e(7801);t.exports=function(t,r){var e=o(t,r);return n(e)?e:void 0}},5924:(t,r,e)=>{var n=e(5569)(Object.getPrototypeOf,Object);t.exports=n},9607:(t,r,e)=>{var n=e(2705),o=Object.prototype,a=o.hasOwnProperty,i=o.toString,s=n?n.toStringTag:void 0;t.exports=function(t){var r=a.call(t,s),e=t[s];try{t[s]=void 0;var n=!0}catch(u){}var o=i.call(t);return n&&(r?t[s]=e:delete t[s]),o}},7801:t=>{t.exports=function(t,r){return null==t?void 0:t[r]}},1789:(t,r,e)=>{var n=e(4536);t.exports=function(){this.__data__=n?n(null):{},this.size=0}},401:t=>{t.exports=function(t){var r=this.has(t)&&delete this.__data__[t];return this.size-=r?1:0,r}},7667:(t,r,e)=>{var n=e(4536),o=Object.prototype.hasOwnProperty;t.exports=function(t){var r=this.__data__;if(n){var e=r[t];return"__lodash_hash_undefined__"===e?void 0:e}return o.call(r,t)?r[t]:void 0}},1327:(t,r,e)=>{var n=e(4536),o=Object.prototype.hasOwnProperty;t.exports=function(t){var r=this.__data__;return n?void 0!==r[t]:o.call(r,t)}},1866:(t,r,e)=>{var n=e(4536);t.exports=function(t,r){var e=this.__data__;return this.size+=this.has(t)?0:1,e[t]=n&&void 0===r?"__lodash_hash_undefined__":r,this}},8517:(t,r,e)=>{var n=e(3118),o=e(5924),a=e(5726);t.exports=function(t){return"function"!=typeof t.constructor||a(t)?{}:n(o(t))}},5776:t=>{var r=/^(?:0|[1-9]\d*)$/;t.exports=function(t,e){var n=typeof t;return!!(e=null==e?9007199254740991:e)&&("number"==n||"symbol"!=n&&r.test(t))&&t>-1&&t%1==0&&t<e}},6612:(t,r,e)=>{var n=e(7813),o=e(8612),a=e(5776),i=e(3218);t.exports=function(t,r,e){if(!i(e))return!1;var s=typeof r;return!!("number"==s?o(e)&&a(r,e.length):"string"==s&&r in e)&&n(e[r],t)}},7019:t=>{t.exports=function(t){var r=typeof t;return"string"==r||"number"==r||"symbol"==r||"boolean"==r?"__proto__"!==t:null===t}},5346:(t,r,e)=>{var n,o=e(4429),a=(n=/[^.]+$/.exec(o&&o.keys&&o.keys.IE_PROTO||""))?"Symbol(src)_1."+n:"";t.exports=function(t){return!!a&&a in t}},5726:t=>{var r=Object.prototype;t.exports=function(t){var e=t&&t.constructor;return t===("function"==typeof e&&e.prototype||r)}},7040:t=>{t.exports=function(){this.__data__=[],this.size=0}},4125:(t,r,e)=>{var n=e(8470),o=Array.prototype.splice;t.exports=function(t){var r=this.__data__,e=n(r,t);return!(e<0)&&(e==r.length-1?r.pop():o.call(r,e,1),--this.size,!0)}},2117:(t,r,e)=>{var n=e(8470);t.exports=function(t){var r=this.__data__,e=n(r,t);return e<0?void 0:r[e][1]}},7518:(t,r,e)=>{var n=e(8470);t.exports=function(t){return n(this.__data__,t)>-1}},4705:(t,r,e)=>{var n=e(8470);t.exports=function(t,r){var e=this.__data__,o=n(e,t);return o<0?(++this.size,e.push([t,r])):e[o][1]=r,this}},4785:(t,r,e)=>{var n=e(1989),o=e(8407),a=e(7071);t.exports=function(){this.size=0,this.__data__={hash:new n,map:new(a||o),string:new n}}},1285:(t,r,e)=>{var n=e(5050);t.exports=function(t){var r=n(this,t).delete(t);return this.size-=r?1:0,r}},6e3:(t,r,e)=>{var n=e(5050);t.exports=function(t){return n(this,t).get(t)}},9916:(t,r,e)=>{var n=e(5050);t.exports=function(t){return n(this,t).has(t)}},5265:(t,r,e)=>{var n=e(5050);t.exports=function(t,r){var e=n(this,t),o=e.size;return e.set(t,r),this.size+=e.size==o?0:1,this}},4536:(t,r,e)=>{var n=e(852)(Object,"create");t.exports=n},3498:t=>{t.exports=function(t){var r=[];if(null!=t)for(var e in Object(t))r.push(e);return r}},1167:(t,r,e)=>{t=e.nmd(t);var n=e(1957),o=r&&!r.nodeType&&r,a=o&&t&&!t.nodeType&&t,i=a&&a.exports===o&&n.process,s=function(){try{var t=a&&a.require&&a.require("util").types;return t||i&&i.binding&&i.binding("util")}catch(r){}}();t.exports=s},2333:t=>{var r=Object.prototype.toString;t.exports=function(t){return r.call(t)}},5569:t=>{t.exports=function(t,r){return function(e){return t(r(e))}}},5357:(t,r,e)=>{var n=e(6874),o=Math.max;t.exports=function(t,r,e){return r=o(void 0===r?t.length-1:r,0),function(){for(var a=arguments,i=-1,s=o(a.length-r,0),u=Array(s);++i<s;)u[i]=a[r+i];i=-1;for(var c=Array(r+1);++i<r;)c[i]=a[i];return c[r]=e(u),n(t,this,c)}}},5639:(t,r,e)=>{var n=e(1957),o="object"==typeof self&&self&&self.Object===Object&&self,a=n||o||Function("return this")();t.exports=a},6390:t=>{t.exports=function(t,r){if(("constructor"!==r||"function"!=typeof t[r])&&"__proto__"!=r)return t[r]}},61:(t,r,e)=>{var n=e(6560),o=e(1275)(n);t.exports=o},1275:t=>{var r=Date.now;t.exports=function(t){var e=0,n=0;return function(){var o=r(),a=16-(o-n);if(n=o,a>0){if(++e>=800)return arguments[0]}else e=0;return t.apply(void 0,arguments)}}},7465:(t,r,e)=>{var n=e(8407);t.exports=function(){this.__data__=new n,this.size=0}},3779:t=>{t.exports=function(t){var r=this.__data__,e=r.delete(t);return this.size=r.size,e}},7599:t=>{t.exports=function(t){return this.__data__.get(t)}},4758:t=>{t.exports=function(t){return this.__data__.has(t)}},4309:(t,r,e)=>{var n=e(8407),o=e(7071),a=e(3369);t.exports=function(t,r){var e=this.__data__;if(e instanceof n){var i=e.__data__;if(!o||i.length<199)return i.push([t,r]),this.size=++e.size,this;e=this.__data__=new a(i)}return e.set(t,r),this.size=e.size,this}},346:t=>{var r=Function.prototype.toString;t.exports=function(t){if(null!=t){try{return r.call(t)}catch(e){}try{return t+""}catch(e){}}return""}},5703:t=>{t.exports=function(t){return function(){return t}}},7813:t=>{t.exports=function(t,r){return t===r||t!=t&&r!=r}},6557:t=>{t.exports=function(t){return t}},5694:(t,r,e)=>{var n=e(9454),o=e(7005),a=Object.prototype,i=a.hasOwnProperty,s=a.propertyIsEnumerable,u=n(function(){return arguments}())?n:function(t){return o(t)&&i.call(t,"callee")&&!s.call(t,"callee")};t.exports=u},1469:t=>{var r=Array.isArray;t.exports=r},8612:(t,r,e)=>{var n=e(3560),o=e(1780);t.exports=function(t){return null!=t&&o(t.length)&&!n(t)}},9246:(t,r,e)=>{var n=e(8612),o=e(7005);t.exports=function(t){return o(t)&&n(t)}},4144:(t,r,e)=>{t=e.nmd(t);var n=e(5639),o=e(5062),a=r&&!r.nodeType&&r,i=a&&t&&!t.nodeType&&t,s=i&&i.exports===a?n.Buffer:void 0,u=(s?s.isBuffer:void 0)||o;t.exports=u},3560:(t,r,e)=>{var n=e(4239),o=e(3218);t.exports=function(t){if(!o(t))return!1;var r=n(t);return"[object Function]"==r||"[object GeneratorFunction]"==r||"[object AsyncFunction]"==r||"[object Proxy]"==r}},1780:t=>{t.exports=function(t){return"number"==typeof t&&t>-1&&t%1==0&&t<=9007199254740991}},3218:t=>{t.exports=function(t){var r=typeof t;return null!=t&&("object"==r||"function"==r)}},7005:t=>{t.exports=function(t){return null!=t&&"object"==typeof t}},8630:(t,r,e)=>{var n=e(4239),o=e(5924),a=e(7005),i=Function.prototype,s=Object.prototype,u=i.toString,c=s.hasOwnProperty,p=u.call(Object);t.exports=function(t){if(!a(t)||"[object Object]"!=n(t))return!1;var r=o(t);if(null===r)return!0;var e=c.call(r,"constructor")&&r.constructor;return"function"==typeof e&&e instanceof e&&u.call(e)==p}},6719:(t,r,e)=>{var n=e(8749),o=e(1717),a=e(1167),i=a&&a.isTypedArray,s=i?o(i):n;t.exports=s},1704:(t,r,e)=>{var n=e(4636),o=e(313),a=e(8612);t.exports=function(t){return a(t)?n(t,!0):o(t)}},2492:(t,r,e)=>{var n=e(2980),o=e(1463)((function(t,r,e){n(t,r,e)}));t.exports=o},5062:t=>{t.exports=function(){return!1}},9881:(t,r,e)=>{var n=e(8363),o=e(1704);t.exports=function(t){return n(t,o(t))}}}]);