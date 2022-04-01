import fs from 'fs'
import YAML from 'yaml'
import ip from 'ip'
export default function (req, res, next) {
    // req is the Node.js http request object
    const conf = fs.readFileSync('/Users/maple/.cam.conf', 'utf8')
    var user_config = YAML.parse(conf)
    user_config['webserver'] = ip.address()
    res.write(JSON.stringify({ user_config }))
    res.end()
  
    // res is the Node.js http response object
  
    // next is a function to call to invoke the next middleware
    // Don't forget to call next at the end if your middleware is not an endpoint!
    next()
  }