/*global module, require */
module.exports = function( grunt ) {
    'use strict';

    grunt.initConfig({
            pkg: grunt.file.readJSON('package.json'),
            jasmine: {
                dev: {
                    src: 'src/breeze.dataservice.neo4j.js',
                    options: {
                        specs: 'test/specs/dev/**/*spec.js',
                        keepRunner: true,
                        template: require('grunt-template-jasmine-requirejs'),
                        templateOptions: {
                            requireConfig: requireConfig
                        },
                        vendor: ['node_modules/jasmine-ajax/lib/mock-ajax.js']
                    }
                }
            }
        }
    );

// Loading plugin(s)
    grunt.loadNpmTasks('grunt-contrib-jasmine');

    grunt.registerTask('default', ['jasmine:dev']);

};
