Gem::Specification.new do |s|
  s.name = %q{kafka-rb}
  s.version = "0.0.16"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Alejandro Crosa", "Stefan Mees", "Tim Lossen", "Liam Stewart"]
  s.autorequire = %q{kafka-rb}
  s.date = Time.now.strftime("%Y-%m-%d")
  s.description = %q{kafka-rb allows you to produce and consume messages using the Kafka distributed publish/subscribe messaging service.}
  s.extra_rdoc_files = ["LICENSE"]
  s.files = ["LICENSE", "README.md", "Rakefile"] + Dir.glob("lib/**/*.rb")
  s.test_files = Dir.glob("spec/**/*.rb")
  s.homepage = %q{http://github.com/acrosa/kafka-rb}
  s.require_paths = ["lib"]
  s.summary = %q{A Ruby client for the Kafka distributed publish/subscribe messaging service}
  s.executables = ["kafka-consumer", "kafka-publish"]

  if s.respond_to? :specification_version then
    current_version = Gem::Specification::CURRENT_SPECIFICATION_VERSION
    s.specification_version = 3

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_development_dependency(%q<rspec>, [">= 0"])
    else
      s.add_dependency(%q<rspec>, [">= 0"])
    end
  else
    s.add_dependency(%q<rspec>, [">= 0"])
  end
end
