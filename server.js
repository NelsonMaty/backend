// server.js

// BASE SETUP
// ============================================================================
   // call the packages we need
   var express    = require('express');        // call express
   var app        = express();                 // define our app using express
   var pg         = require('pg.js');          // call postgres client

   var bodyParser = require('body-parser');    // body parser used in order to read any json data submitted
   var winston    = require('winston');        // logger
   var async      = require('async');          //function sequence controller

   var logger = new (winston.Logger)({
      transports: [
         new (winston.transports.Console)(
            {json:false, 
             timestamp: function() { 
               return (new Date().toISOString().
                        replace(/T/, ' ').      // replace T with a space
                        replace(/\..+/, '')     // delete the dot and everything after; 
                      );
               }
            }
         ),
      ]
   });

   app.use(bodyParser.urlencoded({ extended: true }));
   app.use(bodyParser.json());

   // setting listening port
   var port = process.env.PORT || 8080;

   // connecting to nahuel database
   var conString = "postgres://postgres@localhost/nahuel_dev";


// ROUTES FOR OUR API
// ===========================================================================

// Enabling 'Access-Control-Allow-Origin'
app.all('*', function(req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "X-Requested-With");
  next();
 });

// retrieve titles
app.get('/api/titles', function(req, res, next) {
   logger.info('Request GET recieved for /api/titles');
   pg.connect(conString, function(err, client, done){
      //Return if an error occurs
      if(err) {
         logger.error('Could not connect to nahuel database');
         return next(err);
      }

      //preparing query
      var sql = ''; // base query
      var careerArray = [];      // response array

      var filters_mapping = {    // database columns mapping and comparison mode 
         institution:     {column_name:'edu_institution_name', strictCompare: false},
         academicUnit:    {column_name:'academic_unit_name',   strictCompare: false},
         careerType:      {column_name:'career_type_name',     strictCompare: true },
         career:          {column_name:'career_name',          strictCompare: false},
         titleType:       {column_name:'title_type_name',      strictCompare: true },
         title:           {column_name:'title',                strictCompare: false}
      };

      var resolution_filters_mapping = {
         resolutionType:   {column_name:'rt.name'          },
         resolutionNumber: {column_name:'number_resolution'},
         resolutionYear:   {column_name:'year_resolution'  }
      }

      var isFirstParam = true;
      // if filtering by resolution
      for (var key in resolution_filters_mapping) { 
         if (!!req.param(key)){
            // building sql query string
            if(isFirstParam) { // we will have to join the view with the resolution tables
               sql +=   "select distinct(vt.title_id) as title_id, edu_institution_name, "+
                           "academic_unit_name, career_code, career_name, title_code, title, "+
                           "title_female_title, title_type_name, title_comment, title_mode_name,"+ 
                           "title_state_code, year_resolution, number_resolution, rt.name "+
                        "from model.v_titles vt " +
                           "left join model.title_resolution tr on vt.title_id = tr.title_id " +
                           "left join model.resolution r on tr.resolution_id = r.id " +
                           "left join model.resolution_type rt on r.type_resolution_id=rt.id " +
                        "where " ;
               isFirstParam = false;
            }
            else
               sql += " and ";
            sql += resolution_filters_mapping[key].column_name + " = '" + req.param(key) + "'";
         }
      }

      // checking which filters were used (if any)
      for (var key in filters_mapping) { 
         if (!!req.param(key)){
            // building sql query string
            if(isFirstParam)
               {sql += "SELECT * from model.v_titles where "; isFirstParam = false;}
            else
               sql += " and ";
            if(filters_mapping[key].strictCompare)
               sql += filters_mapping[key].column_name + " = '" + req.param(key) + "'";
            else
               sql += filters_mapping[key].column_name + " ilike '%" + req.param(key) +"%'";
         }
      }


      // if filtering by title states
      if(req.param('titleStates')){
         var jsonStates = JSON.parse(req.param('titleStates'));
         var statesArray = [];
         
         for (var k in jsonStates)
            if(jsonStates[k])
               statesArray.push(k);

         if(statesArray.length > 0){
            var sql_like = statesArray.join('","');
            sql_like = "title_state_code like any('{\"" + sql_like + "\"}')";

            if(isFirstParam)
               {sql += "SELECT * from model.v_titles where "; isFirstParam = false;}
            else
               sql += " and ";
            sql += sql_like;
         }
      }

      //no parameteres had been sent
      if (isFirstParam){
         logger.info('No params received');
         sql = "SELECT * from model.v_titles";
      }

      //querying database
      client.query(sql, function(err, result) {

         logger.info('Running query: '+sql);
         //Return if an error occurs
         if(err) {
            logger.error('Error running query.' + sql);
            return next(err);
         }

         // Saving result in an array
         result.rows.forEach(
            function(data) {
               //var careerState = getCareerState(data);
               var career = {
                  idTitle: data.title_id,
                  institutionName: data.edu_institution_name,
                  academicUnit: data.academic_unit_name,
                  careerCode: data.career_code,
                  careerName: data.career_name,
                  titleCode: data.title_code,
                  titleName: data.title,
                  titleFemaleName: data.title_female_title,
                  titleType: data.title_type_name,
                  titleComment: data.title_comment,
                  titleMode: data.title_mode_name,
                  state: data.title_state_code
               }
               careerArray.push(career);
            }
         );
         done(); //release the pg client back to the pool 
         res.json(careerArray);
      });
   });
});

// retrieve all institutions
app.get('/api/institutions', function(req, res, next) { 
   logger.info('Request GET recieved for /api/institutions');
   pg.connect(conString, function(err, client, done){
      //Return if an error occurs
      if(err) {
         logger.error('Could not connect to nahuel database');
         return next(err);
      }

      //querying database
      var sql = 'SELECT * from model.edu_institution';
      var institutionArray = [];
      client.query(sql, function(err, result) {
         //Return if an error occurs
         if(err) {
            logger.error('error running query: ' + sql);
            return next(err);
         }

         // Storing result in an array
         result.rows.forEach(
            function(data) {
               //var careerState = getCareerState(data);
               var institution = {
                  institutionCode: data.code,
                  institutionName: data.name,
               }
               institutionArray.push(institution);
            }
         );
         done(); //release the pg client back to the pool 
         res.json(institutionArray);
      });
   });
});

// retrieve the academic units in a tree format
app.get('/api/academicUnitsHierarchy', function(req, res, next) { 
   logger.info('Request GET recieved for /api/academicUnitsHierarchy');
   pg.connect(conString, function(err, client, done){

      //Return if an error occurs
      if(err) {
         logger.error('Could not connect to nahuel database');
         return next(err);
      }

      var auArray = {};
      var academicUnitsHierarchy = {};

      // beggining of series of functions
      async.series(
      [
      // Step number 1: Get all academic units
      function(callback){ 
         var sql = 'SELECT * from model.academic_unit';
         client.query(sql, function(err, result) {

            //Return if an error occurs
            if(err) {
               logger.error('error running query: ' + sql);
               return next(err);
            }

            // get all academic units
            result.rows.forEach(
               function(data) {
                  var au = {
                     auName: data.name,
                     auParent: data.academic_unit_parent_id,
                     auChildren: [],
                  }
                  auArray[data.id] = au;  // adding to academic units dictionary
               }
            );
            //logger.info("First step result:", auArray);
            callback();
         });
      },
      // Step number 2: group careers by academic unit id, then assign them as its children
      function(callback){
         async.forEach(Object.keys(auArray), function(key, callback){
            var sql = "select c.name from model.career c join model.academic_unit au on c.academic_unit_id=au.id where au.id='"+key+"'";
            client.query(sql, function(err,result){
               //Return if an error occurs
               if(err) {
                  logger.error('error running query: ' + sql);
                  return next(err);
               }
               result.rows.forEach(
                  function(data) {
                     auArray[key].auChildren.push({"name":data.name});
                  }
               );
               //logger.info(key, auArray[key].auChildren);
               callback();
            });
         },callback);
      },
      // Step number 3: build the academic unit tree hierarchy
      function(callback){
         for (var id in auArray){
            //Academic unit dictionary
            academicUnitsHierarchy[id] = {"name":auArray[id].auName, "parent":auArray[id].auParent, "children": auArray[id].auChildren};
            //logger.info(id, academicUnitsHierarchy[id]);
         }
         //place each academic unit where it belongs
         for (var id in academicUnitsHierarchy){
            var node = academicUnitsHierarchy[id];
            if (!!academicUnitsHierarchy[node.parent]){ // if the au has a parent
               var parentID = node.parent;
               delete node.parent; //ids wont be shown in the final result
               academicUnitsHierarchy[parentID].children.push(node); // set the au as a child
               //logger.info(academicUnitsHierarchy[parentID]);
               delete academicUnitsHierarchy[id]; // the au is no longer a root node
            }
         }
         callback();
      },
      ],
      // Final step: release the DB client and respond
      function(err, results){ 
         done();
         response = [];
         for (var item in academicUnitsHierarchy){
            delete academicUnitsHierarchy[item].parent;
            response.push(academicUnitsHierarchy[item]);
         }
         res.json(response);
      }
      );
   });
});

// retrieve all academic units
app.get('/api/academicUnits', function(req, res, next) { 
   logger.info('Request GET recieved for /api/academicUnits');
   pg.connect(conString, function(err, client, done){
      //Return if an error occurs
      if(err) {
         logger.error('Could not connect to nahuel database');
         return next(err);
      }

      //querying database
      var sql = 'SELECT * from model.academic_unit';
      var auArray = [];
      client.query(sql, function(err, result) {
         //Return if an error occurs
         if(err) {
            logger.error('error running query: ' + sql);
            return next(err);
         }

         // Storing result in an array
         result.rows.forEach(
            function(data) {
               //var careerState = getCareerState(data);
               var au = {
                  auCode: data.code,
                  auName: data.name,
               }
               auArray.push(au);
            }
         );
         done(); //release the pg client back to the pool 
         res.json(auArray);
      });
   });
});

// retrieve all career types
app.get('/api/careerTypes', function(req, res, next) { 
   logger.info('Request GET recieved for /api/careerTypes');
   pg.connect(conString, function(err, client, done){
      //Return if an error occurs
      if(err) {
         logger.error('Could not connect to nahuel database');
         return next(err);
      }

      //querying database
      var sql = 'SELECT * from model.career_type';
      var responseArray = [];
      client.query(sql, function(err, result) {
         //Return if an error occurs
         if(err) {
            logger.error('error running query: ' + sql);
            return next(err);
         }

         // Storing result in an array
         result.rows.forEach(
            function(data) {
               //var careerState = getCareerState(data);
               var dto = {
                  careerTypeCode: data.code,
                  careerTypeName: data.name,
               }
               responseArray.push(dto);
            }
         );
         done(); //release the pg client back to the pool 
         res.json(responseArray);
      });
   });
});

// retrieve all title types
app.get('/api/titleTypes', function(req, res, next) { 
   logger.info('Request GET recieved for /api/titleTypes');
   pg.connect(conString, function(err, client, done){
      //Return if an error occurs
      if(err) {
         logger.error('Could not connect to nahuel database');
         return next(err);
      }

      //querying database
      var sql = 'SELECT * from model.title_type';
      var responseArray = [];
      client.query(sql, function(err, result) {
         //Return if an error occurs
         if(err) {
            logger.error('error running query: ' + sql);
            return next(err);
         }

         // Storing result in an array
         result.rows.forEach(
            function(data) {
               //var careerState = getCareerState(data);
               var dto = {
                  titleTypeCode: data.code,
                  titleTypeName: data.name,
               }
               responseArray.push(dto);
            }
         );
         done(); //release the pg client back to the pool 
         res.json(responseArray);
      });
   });
});

// retrieve all institutions
app.get('/api/careers', function(req, res, next) { 
   logger.info('Request GET recieved for /api/careers');
   pg.connect(conString, function(err, client, done){
      //Return if an error occurs
      if(err) {
         logger.error('Could not connect to nahuel database');
         return next(err);
      }

      //querying database
      var sql = 'SELECT * from model.career';
      var responseArray = [];
      client.query(sql, function(err, result) {
         //Return if an error occurs
         if(err) {
            logger.error('error running query: ' + sql);
            return next(err);
         }

         // Storing result in an array
         result.rows.forEach(
            function(data) {
               //var careerState = getCareerState(data);
               var dto = {
                  careerCode: data.code,
                  careerName: data.name,
               }
               responseArray.push(dto);
            }
         );
         done(); //release the pg client back to the pool 
         res.json(responseArray);
      });
   });
});

// retrieve all institutions
app.get('/api/resolutionTypes', function(req, res, next) { 
   logger.info('Request GET recieved for /api/resolutionTypes');
   pg.connect(conString, function(err, client, done){
      //Return if an error occurs
      if(err) {
         logger.error('Could not connect to nahuel database');
         return next(err);
      }

      //querying database
      var sql = 'SELECT * from model.resolution_type';
      var responseArray = [];
      client.query(sql, function(err, result) {
         //Return if an error occurs
         if(err) {
            logger.error('error running query: ' + sql);
            return next(err);
         }

         // Storing result in an array
         result.rows.forEach(
            function(data) {
               //var careerState = getCareerState(data);
               var dto = {
                  resolutionTypeCode: data.code,
                  resolutionTypeName: data.name,
               }
               responseArray.push(dto);
            }
         );
         done(); //release the pg client back to the pool 
         res.json(responseArray);
      });
   });
});

// retrieve resolutions
app.get('/api/resolutions', function(req, res, next) { 
   logger.info('Request GET recieved for /api/resolutions');
   pg.connect(conString, function(err, client, done){
      //Return if an error occurs
      if(err) {
         logger.error('Could not connect to nahuel database');
         return next(err);
      }
      //querying database
      var sql = 'select * from model.title_resolution tr '+
                  'left join model.resolution r on tr.resolution_id=r.id '+
                  'left join model.resolution_type rt on r.type_resolution_id = rt.id';
      if (!!req.param('idTitle')){
         sql += " where tr.title_id = '"+req.param('idTitle')+"'";
      }
      var responseArray = [];
      client.query(sql, function(err, result) {
         //Return if an error occurs
         if(err) {
            logger.error('error running query: ' + sql);
            return next(err);
         }

         // Storing result in an array
         result.rows.forEach(
            function(data) {
               //var careerState = getCareerState(data);
               var dto = {
                  resolutionTypeName: data.name,
                  resolutionNumber: data.number_resolution,
                  resolutionYear: data.year_resolution,
                  resolutionDate: data.date_resolution
               }
               responseArray.push(dto);
            }
         );
         done(); //release the pg client back to the pool 
         res.json(responseArray);
      });
   });
});

// update title info
app.post('/api/title', function(req, res, next) {
   logger.info('Request POST recieved for /api/title');
   pg.connect(conString, function(err, client, done){
      //Return if an error occurs
      if(err) {
         logger.error('Could not connect to nahuel database');
         return next(err);
      }
      async.series([
         function(cb) {
            logger.info('Beggining transaction');
            client.query('begin work', cb);
         },
         function(cb){
            var title = req.body.title;
            var sql = "update model.title "+
                        "set code=$2, title=$3, female_title=$4, comment=$5 "+
                        "where id=$1";
            var parameters = [title.idTitle, title.titleCode, title.titleName, title.titleFemaleName, title.comment];
            client.query(sql, parameters, cb);
         }
/*         function(cb){
         },
         function(cb){
         }*/
      ],
      // Callback series function
      function(err, result) {
         if(err) {
            logger.error('Performing rollback.', err);
            return client.query('rollback work', function() {
               logger.info("Rollback completed");
               done();
               next(err);
            });
         }
         client.query('commit work', function(err, result) {
            done();
            logger.info("Commited");
            res.json({status: 'ok', message: 'Successful update'});
         });
      });
      
   });
});

// START THE SERVER
// ============================================================================
   app.listen(port);
   logger.info('Nahuel listening on port ' + port);