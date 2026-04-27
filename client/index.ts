/**
 * module_template module entry point.
 * Exports routes for all module_template pages and the navigation sidebar item.
 * 
 * Routes:
 *    module_template/home - home
 */

import type { SidebarNavItem, ModuleRoute } from "@app-types/navigation";

export const base_route = "/module_template";

export const routes: ModuleRoute[] = [
  { 
    path: base_route, 
    load: () => import("./home") 
  },
];

export const navItem: SidebarNavItem = {
  id: "module-template-home",
  title: "Module Template",
  section: "module",
  order: -1,
  children: [
    { 
      id: "module-template-home", 
      title: "Home", 
      path: base_route, 
      section: "module", 
      order: -2 
    },
  ]
};
