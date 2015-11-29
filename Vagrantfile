# -*- mode: ruby -*-

# vi: set ft=ruby :

boxes = [
    {
        :name => "source",
        :ip => "10.10.10.2",
        :mem => "512",
        :cpu => "1",
        #:command => "sudo python3 source.py 0.0.0.0 80"
    },
    {
        :name => "p1",
        :ip => "10.10.10.3",
        :mem => "512",
        :cpu => "1"
    },
    {
        :name => "p2",
        :ip => "10.10.10.4",
        :mem => "512",
        :cpu => "1"
    },
    {
        :name => "p3",
        :ip => "10.10.10.5",
        :mem => "512",
        :cpu => "1"
    },
    {
        :name => "dest",
        :ip => "10.10.10.6",
        :mem => "512",
        :cpu => "1"
    }

]

Vagrant.configure(2) do |config|

  config.vm.box = "ubuntu/trusty64"
  config.vm.synced_folder ".", "/home/vagrant/"

  boxes.each do |opts|
    config.vm.define opts[:name] do |config|
      config.vm.hostname = opts[:name]

      config.vm.provider "virtualbox" do |v|
        v.customize ["modifyvm", :id, "--memory", opts[:mem]]
#        v.customize ["modifyvm", :id, "--cpus", opts[:cpu]]
      end
#     config.vm.provision :shell, inline = opts[:command]
      config.vm.network "private_network", ip: opts[:ip]
    end
  end
end