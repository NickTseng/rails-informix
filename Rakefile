require 'rubygems'
require 'rake'
require 'rubygems/package_task'

PKG_NAME = 'activerecord-informix-adapter'
PKG_VERSION = "1.2.2"

spec = Gem::Specification.new do |s|
  s.name = PKG_NAME
  s.summary = 'Informix adapter for Active Record'
  s.description = 'Active Record adapter for connecting to an IBM Informix database'
  s.version = PKG_VERSION

  s.add_dependency 'activerecord', '>= 3.2.11'
  s.add_dependency 'ruby-informix', '>= 0.7.3'
  s.require_path = 'lib'

  s.files = %w(lib/active_record/connection_adapters/informix_adapter.rb lib/arel/visitors/informix.rb)

  s.author = 'Gerardo Santana Gomez Garrido, Felipe Mathies, Nicola Bernardini'
  s.email = 'nic.bern@tiscali.it'
  s.homepage = 'http://rails-informix.rubyforge.org/'
  s.rubyforge_project = 'rails-informix'
end

Gem::PackageTask.new(spec) do |p|
  p.gem_spec = spec
  p.need_tar = true
  p.need_zip = true
end
