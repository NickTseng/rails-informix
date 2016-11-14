PKG_NAME = 'activerecord-informix-adapter' unless defined?(PKG_NAME)
PKG_VERSION = "1.2.1" unless defined?(PKG_VERSION)

Gem::Specification.new do |s|
  s.name = PKG_NAME
  s.summary = 'Informix adapter for Active Record'
  s.description = 'Active Record adapter for connecting to an IBM Informix database'
  s.version = PKG_VERSION

  s.add_dependency 'activerecord', '>= 3.2.13'
  s.add_dependency 'ruby-informix', '~> 0.8'
  s.require_path = 'lib'

  s.files = %w(lib/active_record/connection_adapters/informix_adapter.rb)

  s.author = 'Gerardo Santana Gomez Garrido'
  s.email = 'gerardo.santana@gmail.com'
  s.homepage = 'http://rails-informix.rubyforge.org/'
  s.rubyforge_project = 'rails-informix'
end
