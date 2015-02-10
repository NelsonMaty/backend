// server.js

// BASE SETUP
// ============================================================================
   // call the packages we need
   var express    = require('express');        // call express
   var app        = express();                 // define our app using express
   var pg         = require('pg.js');             // call postgres client

   var bodyParser = require('body-parser');
   var winston    = require('winston'); // logger
   var async      = require('async');

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

   // configure app to use bodyParser()
   // this will let us get the data from a POST
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
   logger.info('Request recieved for /api/titles');
   pg.connect(conString, function(err, client, done){
      //Return if an error occurs
      if(err) {
         logger.error('Could not connect to nahuel database');
         return next(err);
      }

      //preparing query
      var sql = 'SELECT * from model.v_career_title'; // base query
      var careerArray = [];      // response array

      var filters_mapping = {    // database columns mapping and comparison mode 
         institution:     {column_name:'edu_institution_name', strictCompare: false},
         academicUnit:    {column_name:'academic_unit_name',   strictCompare: false},
         careerType:      {column_name:'career_type_name',     strictCompare: true},
         career:          {column_name:'career_name',          strictCompare: false},
         titleType:       {column_name:'title_type_name',      strictCompare: true},
         title:           {column_name:'title',                strictCompare: false},
         resolutionType:  {column_name:'resolution_type_name', strictCompare: true},
         resolutionNumber:{column_name:'titulo_resol_num',     strictCompare: true},
         resolutionYear:  {column_name:'titulo_resol_anio',    strictCompare: true}
      };

      // checking which filters were used (if any)
      var isFirstParam = true;
      for (var key in filters_mapping) { 

         if (!!req.param(key)){
            // building sql query string
            if(isFirstParam)
               {sql += " where "; isFirstParam = false;}
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
               {sql += " where "; isFirstParam = false;}
            else
               sql += " and ";
            sql += sql_like;
         }
      }

      //no parameteres had been sent
      if (isFirstParam)
         logger.info('No params received');

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
                  academicUnit: data.academic_unit_name,
                  careerCode: data.career_code,
                  careerName: data.career_name,
                  titleCode: data.title_code,
                  titleName: data.title,
                  titleType: data.title_type_name,
                  careerMode: data.title_mode_name,
                  //state: careerState
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
   logger.info('Request recieved for /api/institutions');
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
   logger.info('Request recieved for /api/academicUnitsHierarchy');
   pg.connect(conString, function(err, client, done){

      //Return if an error occurs
      if(err) {
         logger.error('Could not connect to nahuel database');
         return next(err);
      }

      var auArray = {};
      var academicUnitsHierarchy = {};

      async.series(
      [function(callback){
         //querying database
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
                     auID: data.id,
                     auParent: data.academic_unit_parent_id,
                     auChildren: [],
                  }
                  auArray[data.name] = au;
               }
            );
            callback();
         });
      },
      function(callback){
         // get all careers grouped by academic unit
         async.forEach(Object.keys(auArray), function(key, callback){
            var sql = "select career_name from model.v_career_title where academic_unit_name='"+key+"' group by career_name";
            client.query(sql, function(err,result){
               //Return if an error occurs
               if(err) {
                  logger.error('error running query: ' + sql);
                  return next(err);
               }
               result.rows.forEach(
                  function(data) {
                     auArray[key].auChildren.push({"name":data.career_name});
                  }
               );
               callback();
            });
         },callback);
      },
      function(callback){
         for (var item in auArray){
            academicUnitsHierarchy[auArray[item].auID] = {"name":item, "parent":auArray[item].auParent, "children": auArray[item].auChildren};
         }
         for (var id in academicUnitsHierarchy){
            var node = academicUnitsHierarchy[id];
            if (!!node.parent){
               var parentID = node.parent;
               delete node.parent;
               academicUnitsHierarchy[parentID].children.push(node);
               delete academicUnitsHierarchy[id];
            }
         }
         callback();
      },
      ],
      function(err, results){ // callback series function
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
   logger.info('Request recieved for /api/academicUnits');
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
   logger.info('Request recieved for /api/careerTypes');
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
   logger.info('Request recieved for /api/titleTypes');
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
   logger.info('Request recieved for /api/careers');
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
   logger.info('Request recieved for /api/resolutionTypes');
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

// START THE SERVER
// ============================================================================
   app.listen(port);
   logger.info('Nahuel listening on port ' + port);