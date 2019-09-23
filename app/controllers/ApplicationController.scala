package controllers

import javax.inject.Inject

import play.api.libs.json.Json
import play.api.mvc._
import services.DBConnectionService

/**
  * A very small controller that renders a home page.
  */
class ApplicationController @Inject()( dBConnectionService: DBConnectionService) extends Controller {

  def index = Action { implicit request =>
    Ok(Json.obj("status" -> "Hello World"))
  }
}


