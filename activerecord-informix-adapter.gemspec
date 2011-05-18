PKG_NAME = 'activerecord-informix-adapter'
PKG_VERSION = "1.1.1"

Gem::Specification.new do |s|
  s.name = PKG_NAME
  s.summary = 'Informix adapter for Active Record'
  s.description = 'Active Record adapter for connecting to an IBM Informix database'
  s.version = PKG_VERSION

  s.add_dependency 'activerecord', '>= 1.15.4.7707'
  s.add_dependency 'ruby-informix', '>= 0.7.3'
  s.require_path = 'lib'

  s.files = %w(lib/active_record/connection_adapters/informix_adapter.rb lib/arel/visitors/informix.rb)

  s.author = 'Gerardo Santana Gomez Garrido'
  s.email = 'gerardo.santana@gmail.com'
  s.homepage = 'http://rails-informix.rubyforge.org/'
  s.rubyforge_project = 'rails-informix'
end