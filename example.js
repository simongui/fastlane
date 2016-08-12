var dict = {};
dict["one"] = 1;
dict["two"] = 2;

function handleEvent(evt, evt_type) {
    // console.log(xinspect(evt));
    // msg = String.format("JS: evt_type: {0}\ttime: {1}\terror: {2}\tquery: {3}\n", evt_type, evt.GetExecutionTime(), evt.GetErrorCode(), evt.GetQuery());
    // console.log(msg)
    // console.log(evt_type)

    switch (evt_type) {
      case "xidevent":
        break;
      case "queryevent":
      	// Query event.
        // console.log(xinspect(evt));

        // console.log(evt.ExecutionTime)
        bin2String(evt.Query)
        //console.log(bin2String(evt.Query))
        // console.log(evt.Query)
        // console.log("\n")
        break;
      case "intvarevent":
        // last insert_id  if statement based replication.
        break;
      case "writeevent":
        // Write (insert) event.
        break;
      case "deleteevent":
        // Delete event.
        break;
      case "updateevent":
        // Update event.
        break;

    }
}

function xinspect(o,i){
    if(typeof i=='undefined')i='';
    if(i.length>50)return '[MAX ITERATIONS]';
    var r=[];
    for(var p in o){
        var t=typeof o[p];
        r.push(i+'"'+p+'" ('+t+') => '+(t=='object' ? 'object:'+xinspect(o[p],i+'  ') : o[p]+''));
    }
    return r.join(i+'\n');
}

if (!String.format) {
  String.format = function(format) {
    var args = Array.prototype.slice.call(arguments, 1);
    return format.replace(/{(\d+)}/g, function(match, number) {
      return typeof args[number] != 'undefined'
        ? args[number]
        : match
      ;
    });
  };
}

function bin2String(array){
  var result = "";

  try {
    for(var i = 0; i < array.length; ++i){
  		result += (String.fromCharCode(array[i]));
  	}
  }
  catch(err) {
    console.log(err)
  }
	return result;
}
